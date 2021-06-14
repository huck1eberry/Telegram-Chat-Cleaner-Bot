from datetime import datetime, timedelta, time
import functools
import json
import logging
import traceback
from logging import Logger
import sys
from dataclasses import dataclass
import pytz
from telegram import message
from bidict import bidict
from telegram.chatinvitelink import ChatInviteLink

from messages_repo import MessagesRepo, MessageEntity
from typing import Dict, Optional, List, Set, Callable, Any

from telegram import Update, Chat
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackContext, PicklePersistence, updater
from telegram.user import User
from telegram.message import Message
from telegram.error import (TelegramError, Unauthorized, BadRequest, TimedOut, ChatMigrated, NetworkError)

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level = logging.INFO
)
logger: Logger = logging.getLogger(__name__)

@dataclass
class Webhook():
    """Webhook configuration"""
    listen: str
    port: int
    webhook_base_url: str

@dataclass
class Config():
    """Represents complete bot configuration."""
    bot_token: str
    """Bot token received from the @botfather"""
    db_path: str
    """Relative path to the DB-file that is used to store messages data (see messages_repo)"""
    bot_persistence: str
    """Relative path to the pickle dump file where bot's data is kept."""
    webhook_dict: Optional[Dict] = None
    """Optional webhook configuration. This bot is started in a polling mode if this data is missing."""
    cleanup_time_str: str = None
    """Optional daily cleanup time (UTC). Format: '%H:%M'."""

    @property
    def webhook(self) -> Optional[Webhook]:
        if self.webhook_dict:
            return Webhook(**self.webhook_dict)

    @property
    def cleanup_time(self) -> Optional[time]:
        if self.cleanup_time_str:
            return datetime.strptime(self.cleanup_time_str, '%H:%M').time()

def load_config(config_path: str) -> Config:
    """Load and parse bot configuration file."""
    config_format_example = """
        {
            "bot_token": "<your_bot_token>",
            "db_path": "<path_to_the_database>",
            "bot_persistence": "<persistance_srorage_file_path>",
            "webhook_dict": {
                "listen": "<ip_address>",
                "port": <port>,
                "webhook_base_url": "<webhook_url>"
            },
            "cleanup_time_str": <daily_cleanup_time_UTC>
        }"""
    try:
        with open(config_path, 'r') as config_file:
            config_dict = json.load(config_file)
            return Config(**config_dict)
    except:
        exception = sys.exc_info()
        logger.error(f"Failed to init the bot: {exception[0]}. You may check your configuration file - {config_path}. It supposed to have the following format:\n{config_format_example}\n")
        traceback.print_exc()
        sys.exit("Failed to init the bot. Please, check your 'config.json' file.")

class CleanerBot:
    """Responsible for the """
    def __init__(self, config_path: str):
        config: Config = load_config(config_path)
        self._config = config
        """Initialisation config data."""
        self._messages_repo = MessagesRepo(db_path=config.db_path)
        """Messages storage."""

        # picke persistance allows to persist dispatcher's 'bot_data', 'user_data' and 'chat_data' dictionaries
        pickle_persistence = PicklePersistence(filename=config.bot_persistence)
        self._updater = Updater(token=config.bot_token, persistence=pickle_persistence, user_sig_handler=self._signal_handler, use_context=True)
        
        dispatcher = self._updater.dispatcher

        # setup active groups ID storage in 'bot_data' dict
        # TODO could these cause some 'shared state' issues? have to be reworked, I guess 
        if 'active_groups' not in dispatcher.bot_data:
            dispatcher.bot_data['active_groups'] = set()

        # setup groups joiner dicts 
        if 'group_joiner_chats' not in dispatcher.bot_data:
            dispatcher.bot_data['group_joiner_chats'] = bidict()

        # Registration of supported commands
        # NOTE: if message fileters overlap each other, only first handler will be triggered (order defines what is going to be  triggered)
        dispatcher.add_handler(CommandHandler("start", self._start))
        dispatcher.add_handler(CommandHandler("help", self._help))
        dispatcher.add_handler(CommandHandler("version", self._version))
        dispatcher.add_handler(CommandHandler("restrictions", self._restrictions))
        dispatcher.add_handler(CommandHandler("bot_data", self._bot_data))
        #group joiner functionality
        dispatcher.add_handler(CommandHandler("setup_join_config", self._setup_join_config))
        dispatcher.add_handler(CommandHandler("join", self._join))
        # actual wiping-related functionality
        dispatcher.add_handler(MessageHandler(Filters.status_update.chat_created, self._chat_created))
        dispatcher.add_handler(MessageHandler(Filters.status_update.new_chat_members, self._user_added))
        dispatcher.add_handler(MessageHandler(Filters.status_update.left_chat_member, self._user_removed))
        dispatcher.add_handler(MessageHandler(Filters.status_update.migrate, self._chat_migrated))
        dispatcher.add_handler(CommandHandler("cleanup", self._cleanup))
        # common messages callback, that doesn't do much, but keeps incoming messages
        dispatcher.add_handler(MessageHandler(Filters.all, self._receive_incoming_message))
        # error handler callback 
        dispatcher.add_error_handler(self._error_callback)

    def launch(self):
        """Launches the bot."""
        # perform get_me() API call, to get actual bot data and check if token is correct
        logger.info("Starting the bot.")
        bot_user: User = self._updater.dispatcher.bot.get_me()
        logger.info(f"Successfully received bot's user info: {bot_user}.")
        # keep actual bot name updated, it is used to handle bot addition/kick scenarios (we are using dispatcher's bot_data dictionary to persist some values)
        self._updater.dispatcher.bot_data['bot_name'] = bot_user.name
        # start DB session 
        self._messages_repo.init_session()
        # start the bot
        if self._config.webhook:
            webhook: Webhook = self._config.webhook
            logger.info(f"Starting the following webhook: {webhook}")
            bot_token = self._config.bot_token
            self._updater.start_webhook(listen=webhook.listen, port=webhook.port, url_path=bot_token)
            self._updater.bot.set_webhook(f"{webhook.webhook_base_url}{bot_token}")
        else:
            logger.info("Starting polling.")
            self._updater.start_polling()
        
        # schedule global cleanup (UTC time) if necessary
        daily_cleanup_time: time = self._config.cleanup_time
        if daily_cleanup_time:
            logger.info(f"Shcedulling daily cleanup job at {daily_cleanup_time} time, UTC.")
            self._updater.dispatcher.job_queue.run_daily(self._perform_total_cleanup, 
                time(daily_cleanup_time.hour, daily_cleanup_time.minute, tzinfo=pytz.utc))
        # self._updater.dispatcher.job_queue.run_once(self._perform_total_cleanup, 5)
        
        # blocks the execution
        self._updater.idle()


    # helper methods/properties
    @property
    def _active_groups(self) -> Set[int]:
        """Retieves 'active groups' set from the 'bot_data' dictionary that is persisted by the bot (see bot iniitalisation)"""
        return self._updater.dispatcher.bot_data['active_groups']

    @property
    def _group_joiner_chats(self) -> bidict[int, str]:
        """Retieves 'group joiner chats' dict (chat_id to chat_name) from the 'bot_data' dictionary that is persisted by the bot (see bot iniitalisation)"""
        return self._updater.dispatcher.bot_data['group_joiner_chats']

    def _abandon_chat(self, chat_id: int) -> None:
        """Handle all possible situations when the bot can't work with a chat (when getting kicked or chat is deleted, for exammple)"""
        self._active_groups.discard(chat_id)
        self._messages_repo.delete_chat_messages(chat_id=chat_id)
        if chat_id in self._group_joiner_chats:
            del self._group_joiner_chats[chat_id]

    def _send_status_message(self, chat_id: int, text: str, *args, **kwargs) -> Message:
        """Use this method to send cleanup status messages. Here we can properly react to possible Unauthorized exceptions (for instance when chat is deleted ot got kicked)"""
        try:
            return self._updater.dispatcher.bot.send_message(chat_id=chat_id, text=text, *args, **kwargs)
        except Unauthorized:
            logger.error(f"Failed to send a status message to the chat/{chat_id}; removing it.")
            self._abandon_chat(chat_id=chat_id)

    def _handle_chat_id_migration(self,old_chat_id: int, new_chat_id: int) -> None:
        logger.info(f"The chat id has been updated from {old_chat_id} to {new_chat_id}")
        # update active groups set
        self._active_groups.discard(old_chat_id)
        self._active_groups.add(new_chat_id)
        # message entities should be updated as well
        self._messages_repo.update_chat_id(original_chat_id=old_chat_id, updated_chat_id=new_chat_id)

    def _retain_message(self, message: Message) -> None:
        """Check if the message data (message_id, chat_id, date) should be kept for a further removal"""
        if message.chat.type in (Chat.GROUP, Chat.SUPERGROUP):
            logger.info(f"Keeping the message: {message.message_id}")
            entity = MessageEntity(message_id=message.message_id, chat_id=message.chat_id, timestamp=message.date)
            self._messages_repo.add_message(entity)

    @property
    def _deletion_limit(self) -> datetime:
        """Provides datetime limit that defines if a message could be deleted - it has to be more recent than returned value (old messages can't be deleted via bot API, see /restrictions)"""
        deletion_threshold: datetime = datetime.utcnow() - timedelta(days=2)
        return deletion_threshold

    class Decorators:
        """Declare all internal bot-related decorators here."""
        @classmethod
        def keep_callback_messages(cls, callback: Callable[['CleanerBot', Update, CallbackContext], Any]) -> Callable[['CleanerBot', Update, CallbackContext], Any]:
            """Applied to a handler callbacks this decorator automatically saves incoming and outcoming (if presented) messages into the repo."""
            @functools.wraps(callback)
            def wrap(self: 'CleanerBot', update: Update, context: CallbackContext) -> Message: 
                in_message: Message = update.message
                self._retain_message(in_message)

                out_message = callback(self, update, context)
                if isinstance(out_message, Message):
                    self._retain_message(out_message)

                return out_message
            return wrap

        @classmethod
        def log_messages(cls, callback: Callable[['CleanerBot', Update, CallbackContext], Any]) -> Callable[['CleanerBot', Update, CallbackContext], Any]:
            """Applied to a handler callbacks this decorator automatically logs all incoming and outcomming messages."""
            @functools.wraps(callback)
            def wrap(self: 'CleanerBot', update: Update, context: CallbackContext) -> Message: 
                in_message: Message = update.message
                out_message = callback(self, update, context)
                logger.info(f"Callback log: {callback.__name__}.\nInc. message:\n{in_message}.\nOut. message:\n{out_message}.\n")
                return out_message
            return wrap
        
    # helper/test command handlers
    @Decorators.keep_callback_messages
    def _start(self, update: Update, context: CallbackContext) -> Message:
        """Send a message when the command /start is issued."""
        logger.info("start command")
        return context.bot.send_message(chat_id=-1001131794881, text="Hello there! This is a bot designed to cleanup messages in a group chat. Use /help command to get some additional info.")

    @Decorators.keep_callback_messages
    def _help(self, update: Update, context: CallbackContext) -> Message:
        """Send a message when the command /help is issued."""
        return update.message.reply_text("Use /cleanup to remove recent messages in a group chat (admin rights required).\n\nHappy New Year! <3")
    
    @Decorators.keep_callback_messages
    def _version(self, update: Update, context: CallbackContext) -> Message:
        """Send current bot version."""
        return context.bot.send_message(chat_id=update.message.chat_id, text="0.0.1 (alpha) 2021.01.04")

    @Decorators.keep_callback_messages
    def _restrictions(self, update: Update, context: CallbackContext) -> None:
        """Send a message when the command /help is issued."""
        restrictions_message: str = """Message deletion limitations (see https://core.telegram.org/bots/api#deletemessage):
        - A message can only be deleted if it was sent less than 48 hours ago.
        - A dice message in a private chat can only be deleted if it was sent more than 24 hours ago.
        - Bots can delete outgoing messages in private chats, groups, and supergroups.
        - Bots can delete incoming messages in private chats.
        - Bots granted can_post_messages permissions can delete outgoing messages in channels.
        - If the bot is an administrator of a group, it can delete any message there.
        - If the bot has can_delete_messages permission in a supergroup or a channel, it can delete any message there.
        
        NOTE: Bots aren't able to see messages from other bots regardless of mode (https://core.telegram.org/bots/faq).
        """
        return update.message.reply_text(restrictions_message)

    @Decorators.keep_callback_messages
    def _bot_data(self, update: Update, context: CallbackContext) -> Message:
        """Internal command. Send all 'bot_data' content back to the chat"""
        bot_data: str = str(context.bot_data)
        return update.message.reply_text(text=bot_data, reply_to_message_id=update.message.message_id)

    @Decorators.log_messages
    @Decorators.keep_callback_messages
    def _receive_incoming_message(self, update: Update, context: CallbackContext) -> None:
        """Simple callback that receives and stores it's message."""
        pass

    # joining-related handlers
    # TODO rework & refactor all join funct
    @Decorators.log_messages
    @Decorators.keep_callback_messages
    def _setup_join_config(self, update: Update, context: CallbackContext) -> Message:
        """Adds join mapping for the group."""
        message: Message = update.message
        chat_id = message.chat_id
        if message.chat.type not in (Chat.GROUP, Chat.SUPERGROUP):
            return message.reply_text('This command is not supposed to work here.')

        command_segments: List[str] = message.text.split()
        if len(command_segments) >= 2:
            command_length: int = len(command_segments[0])
            message_length: int = len(message.text)
            chat_name: str = message.text[command_length:message_length].strip()
            is_taken: bool = chat_name in self._group_joiner_chats.inverse
            if is_taken:
                name_owner_id: int = self._group_joiner_chats.inverse[chat_name]
                if name_owner_id != chat_id:
                    return update.message.reply_text(text=f"'{chat_name}' is already taken. Please, use something else.", reply_to_message_id=update.message.message_id)

            overriding: bool = chat_id in self._group_joiner_chats
            self._group_joiner_chats[chat_id] = chat_name   
            if overriding:
                return context.bot.send_message(chat_id=chat_id, text="Chat join parameters have been updated.")
            else:
                return context.bot.send_message(chat_id=chat_id, text="Chat join parameters have been set.")
        else:
            return update.message.reply_text(text=f"Not enough arguments.", reply_to_message_id=update.message.message_id)

    @Decorators.keep_callback_messages
    def _join(self, update: Update, context: CallbackContext) -> Message:
        """Join the group."""
        message: Message = update.message
        command_segments: List[str] = message.text.split()
        if len(command_segments) >= 2:
            command_length: int = len(command_segments[0])
            message_length: int = len(message.text)
            chat_name: str = message.text[command_length:message_length].strip()
            if chat_name in self._group_joiner_chats.inverse:
                joining_chat_id = self._group_joiner_chats.inverse[chat_name]
                try:
                    expire_date: datetime = datetime.utcnow() + timedelta(seconds=60)
                    invite_link: ChatInviteLink = context.bot.create_chat_invite_link(chat_id=joining_chat_id, expire_date=expire_date)
                    update.message.reply_text(text=f"Here is your invite link: {invite_link.invite_link}", reply_to_message_id=update.message.message_id) 
                except BadRequest:
                    logger.error(f"Got 'BadRequest' exception during invite link creation. Most likely due to 'missing rights'.")
                    update.message.reply_text(text=f"Failed to create the link.", reply_to_message_id=update.message.message_id)  
            else:
                return update.message.reply_text(text=f"Can't find the chat.", reply_to_message_id=update.message.message_id)

        else:
            return update.message.reply_text(text=f"Not enough arguments.", reply_to_message_id=update.message.message_id)

    # wiping-related handlers  
    def _chat_migrated(self, update: Update, context: CallbackContext) -> None: 
        """Handles chat_id change update."""
        logger.info(update.message)
        if update.message.chat.type in (Chat.GROUP, Chat.SUPERGROUP):
            message: Message = update.message
            # that's how IDs should be settled according to the docs (https://github.com/python-telegram-bot/python-telegram-bot/wiki/Storing-bot,-user-and-chat-related-data)
            # NOTE: this callback is triggered twice during a single chat ID migration:
            # 1st time with the 'migrate_to_chat_id' and old 'chat_id' (like it's an update from the old chat) and
            # 2nd time with the 'migrate_from_chat_id' and updated 'chat_id' (like it's an update from the new chat)
            old_chat_id: int = message.migrate_from_chat_id or message.chat_id
            new_chat_id: int = message.migrate_to_chat_id or message.chat_id
            self._handle_chat_id_migration(old_chat_id=old_chat_id, new_chat_id=new_chat_id)

    @Decorators.keep_callback_messages
    def _chat_created(self, update: Update, context: CallbackContext) -> Message:
        """New group chat've been created with this bot as one of the initial members.""" 
        # if update.effective_chat.type in (Chat.GROUP, Chat.SUPERGROUP): # no need to check this here: the event itself implies that it could only be a common group (see docs.)
        logger.info(f"New group chat've been created with this bot as one of the initial members: {update.effective_chat.title}")
        self._active_groups.add(update.effective_chat.id)
        return context.bot.send_message(update.effective_chat.id, "Hello, this is a (*group) chat cleaning bot. Please, use /help command to get more info.")

    @Decorators.keep_callback_messages
    def _user_added(self, update: Update, context: CallbackContext) -> Optional[Message]:
        """This bot've been added to a group chat.""" 
        if update.effective_chat.type in (Chat.GROUP, Chat.SUPERGROUP):
            new_users: List[User] = update.message.new_chat_members
            for user in new_users:
                if user.name == context.bot_data['bot_name']:
                    logger.info(f"Bot has beed added to the (super)group: '{update.effective_chat.title}'.")
                    self._active_groups.add(update.effective_chat.id)
                    return context.bot.send_message(update.effective_chat.id, "Hello, this is a (*group) chat cleaning bot. Please, use /help command to get more info.")

    @Decorators.keep_callback_messages
    def _user_removed(self, update: Update, context: CallbackContext) -> None:
        """This bot've been removed from the group chat."""
        if update.effective_chat.type in (Chat.GROUP, Chat.SUPERGROUP):
            removed_user: User = update.message.left_chat_member
            if removed_user.name == context.bot_data['bot_name']:
                logger.info(f"Bot has beed removed from the (super)group: '{update.effective_chat.title}'.")
                self._abandon_chat(chat_id=update.effective_chat.id)

    @Decorators.keep_callback_messages
    def _cleanup(self, update: Update, context: CallbackContext) -> None:
        """Delete all recent messages from this chat"""
        message: Message = update.message
        if message.chat.type in (Chat.GROUP, Chat.SUPERGROUP):
            context.job_queue.run_once(self._perform_chat_cleanup, 2, context=message.chat_id)
        else:
            message.reply_text('This command is not supposed to work here. Use /help to get more info.')

    # cleanup methods
    def _chat_cleanup(self, context: CallbackContext, chat_id: int) -> None:
        """Wipe all recent (see /restrictions) messages for the chat and send the report."""
        logger.info(f"Initiated chat ({chat_id}) cleanup.")
        deletion_threshold: datetime = self._deletion_limit
        recent_messages: List[Message] = self._messages_repo.get_chat_messages(chat_id, deletion_threshold)
        deleted_messages: List[Message] = list()
        unauthorized: bool = False
        for message_entity in recent_messages:
            deleted: bool = False
            try:
                deleted = context.bot.delete_message(chat_id=chat_id, message_id=message_entity.message_id)
            except Unauthorized:
                # most likely bot has been kicked or chat is deleted
                logger.error(f"Got 'Unauthorised' exception during a message deletion. Stopping deletion job.")
                unauthorized = True
                break
            except:
                # TODO should we add some praticular exceptions handling here, like BadReuest (could be triggered if there is no rights for deletion) or Unauthorized (removed from the chat)
                exception = sys.exc_info()
                logger.error(f"Failed to perform message deletion API call for the following message: {message_entity}. {exception[0]}")
                traceback.print_exc()
                
            
            if deleted:
                deleted_messages.append(message_entity)
                self._messages_repo.remove_message(message_entity) # TODO add some sort of bulk deletion in the end? not sure if it is possible though
        
        if unauthorized:
            self._abandon_chat(chat_id=chat_id)
            return

        fails_count : int = len(recent_messages) - len(deleted_messages) 
        report_message: str = f"Removed {len(deleted_messages)} recent messages. Failed to remove: {fails_count}."
        logger.info(report_message)
        message = self._send_status_message(chat_id=chat_id, text=report_message, disable_notification=True)
        self._retain_message(message)

    def _perform_chat_cleanup(self, context: CallbackContext) -> None:
        """A job queue function. Wipes all recent (see /restrictions) messages for a specific chat and sends a report."""
        chat_id: int = context.job.context
        self._chat_cleanup(context, chat_id)

    def _perform_total_cleanup(self, context: CallbackContext) -> None:
        """A job queue function. Wipes all recent (see /restrictions) messages for every (!) active chat."""
        # making a copy of the set, because the origianl one may be modified (removing items) during the iteration (# TODO is it right way to solve this issue?)
        active_groups: Set[int] = self._active_groups.copy()
        for group_chat_id in active_groups:
            try:
                self._chat_cleanup(context, group_chat_id)
            except:
                # TODO emprove error handling
                exception = sys.exc_info()
                logger.error(f"Failed to perform chat/{group_chat_id} cleanup: {exception[0]}. Continuing to work on other chats cleanup.")
                traceback.print_exc()
        
        #at the end of a global cleanup remove outdated messages
        self._messages_repo.remove_outdated_messages(self._deletion_limit)

    # Sidenotes about some common "exceptional" scenarios:
    # telegram.error.Unauthorised exception could be triggered when addressee chat is deleted or 
    # telegram.error.BadRequest may be raised if the bot have no rights to perform intended action (restricted by the admin or admin rights are required)

    def _error_callback(self, update: Update, context: CallbackContext) -> None:
        """Top-level error callback.
        NOTE: if the error is being triggered outside of a handler callback
        (during some queued job execution, for instance) 'update' argument may be None"""
        
        logger.error(f"Error: {context.error}, {type(context.error)}")
        try:
            raise context.error
        except ChatMigrated as e:
            # the chat_id of a group has changed, use e.new_chat_id instead
            # NOTE: perhaps, it's redundant catch, since the bot should process chat migration events in the corresponding update handler (see chat_migrated)
            if update:
                old_chat_id: int = update.message.chat_id
                new_chat_id: int = e.new_chat_id
                self._handle_chat_id_migration(old_chat_id=old_chat_id, new_chat_id=new_chat_id)
        # except Unauthorized as e:
        #     # most likely, our bot has been kicked or blocked; forget about this chat
        #     # NOTE: can't udate 'active_chats' from here: unauchorised chat's ID could not be available from here,
        #     # since update may be None (when the exception is being triggered from a job_queue);
        #     # seems like we have to catch this exception directly while performing some call that may cause this error 
        #     raise e
        # except BadRequest:
        #     # handle malformed requests - read more below!
        # except TimedOut:
        #     # handle slow connection problems
        # except NetworkError:
        #     # handle other connection problems
        # except TelegramError:
        #     # handle all other telegram related errors


    def _signal_handler(self, signum, frame) -> None:
        """Handle system signals. In theory that's where we should close DB-connection and other shutdown-related tasks."""
        logger.info(f"Handling signal: {signum}.")
        self._messages_repo.close_session()
        

if __name__ == '__main__':
    bot = CleanerBot(config_path="config.json")
    bot.launch()
    
