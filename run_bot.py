from telegram.ext import Updater, CommandHandler, MessageHandler, Filters
import bot_db as db
import random
import logging
import datetime
import json
import sys

TRIGGERS = {}
BAN_IDS = []
INIT_TIMESTAMP = datetime.datetime.now()

'''
Commands:
User:
/add <trigger_1>|<trigger_2>|...|<trigger_n>@<text_1>|<text_2>|...|<text_n>
/del <trigger_1>|<trigger_2>|...|<trigger_n>@<text_1>|<text_2>|...|<text_n>
/list <trigger>
/edits  //show recently edited message
/search <keyword1> <keyword2> ...
/stats
/echo <text>
/help
Admin only:
/merge trigger1=>trigger2 //move all text of trigger1 to trigger2
/clear <trigger>
/triggers //list all triggers in this chat

'''


def get_group_admin_ids(bot, chat_id):
    result = bot.get_chat_administrators(chat_id)
    return [admin.user.id for admin in result]


def update_trigger_list(chat_id=None):
    global TRIGGERS
    if chat_id:  # update TRIGGERS[chat_id] only
        TRIGGERS[chat_id] = sorted(db.query_all_triggers_in_chat(chat_id), key=lambda t: len(t), reverse=True)
    else:  # update the whole dict
        result = db.query_all_triggers()
        for record in result:
            if record[1] not in TRIGGERS.keys():
                TRIGGERS[record[1]] = []
            TRIGGERS[record[1]].append(record[0])
        for key in TRIGGERS.keys():
            TRIGGERS[key].sort(key=lambda t: len(t), reverse=True)


def add(bot, update):
    try:
        content = update.message.text.split(' ', 1)[1].split('@', 1)
        triggers = content[0].split('|')
        texts = content[1].split('|')
    except IndexError:
        update.message.reply_text('没东西 add 个大头鬼啦')
        return
    for text in texts:
        if not len(text) <= 140:
            update.message.reply_text('2<=len(trigger)<=140, len(text)<=140')
            return
    for trigger in triggers:
        if not 2 <= len(trigger) <= 140:
            update.message.reply_text('2<=len(trigger)<=140, len(text)<=140')
            return

    result = db.add_trigger_text(triggers, texts, update.message.chat_id)
    if result != db.SUCCESS:
        lines = []
        for item in result:
            lines.append('- %s@%s already exists' % item)
        update.message.reply_text('Result:\n' + '\n'.join(lines))

    update.message.reply_text('done!')
    update_trigger_list(update.message.chat_id)


def delete(bot, update):
    try:
        content = update.message.text.split(' ', 1)[1].split('@', 1)
        triggers = content[0].split('|')
        texts = content[1].split('|')
    except IndexError:
        return
    db.delete_trigger_text(triggers, texts, update.message.chat_id)
    update.message.reply_text('deleted!')
    update_trigger_list(update.message.chat_id)


def list_text(bot, update):
    if update.message.date < INIT_TIMESTAMP:
        return
    try:
        trigger = update.message.text.split(' ', 1)[1]
    except IndexError:
        return

    result = db.query_all_text_of_trigger(trigger, update.message.chat_id)
    if result:
        update.message.reply_text('\n'.join(result))
    else:
        update.message.reply_text('Empty list!')


def merge(bot, update):
    if update.message.from_user.id not in get_group_admin_ids(bot, update.message.chat_id):
        update.message.reply_text('Admin only')
        return
    try:
        content = update.message.text.split(' ', 1)[1].split('=>')
        trigger_from = content[0]
        trigger_to = content[1]
    except IndexError:
        update.message.reply_text('Missing arguments!')
        return
    db.merge_trigger(trigger_from, trigger_to, update.message.chat_id)
    update.message.reply_text('merge done!')


def clear(bot, update):
    if update.message.from_user.id not in get_group_admin_ids(bot, update.message.chat_id):
        update.message.reply_text('Admin only')
        return
    try:
        trigger = update.message.text.split(' ', 1)[1]
    except IndexError:
        return
    db.clear_trigger(trigger, update.message.chat_id)
    update.message.reply_text('cleared!')
    update_trigger_list(update.message.chat_id)


def process_trigger(bot, update):
    if update.message.date < INIT_TIMESTAMP:
        return
    if update.message.from_user.id in BAN_IDS:
        return
    if TRIGGERS.get(update.message.chat_id):
        matched_triggers = []

        for trigger in TRIGGERS.get(update.message.chat_id):
            if trigger in update.message.text:
                matched_triggers.append(trigger)
        if matched_triggers:
            update.message.reply_text(
                db.query_random_trigger_text(random.choice(matched_triggers), update.message.chat_id))


def process_chat_message(bot, update):
    if update.edited_message and not update.edited_message.text.startswith('/'):  # process edited message
        db.log_message(message_id=update.edited_message.message_id, text=update.edited_message.text,
                       chat_id=update.edited_message.chat_id,
                       user_id=update.edited_message.from_user.id, time=update.edited_message.edit_date, edited=True)
    else:
        db.update_user_info(user_id=update.message.from_user.id, chat_id=update.message.chat_id,
                            first_name=update.message.from_user.first_name,
                            last_name=update.message.from_user.last_name)  # update user info
        db.update_count(update.message.from_user.id, update.message.chat_id)  # update count
        if update.message.text and not update.message.text.startswith('/'):  # log text message(except commands)
            db.log_message(message_id=update.message.message_id, text=update.message.text,
                           chat_id=update.message.chat_id,
                           user_id=update.message.from_user.id, time=update.message.date)

        if update.message.new_chat_member:  # entering group trigger
            update.message.reply_text('新人请发红包,支付宝QQ微信都可以')
        if update.message.left_chat_member:  # left group trigger
            update.message.reply_text('@%s 跑了!' % update.message.left_chat_member.username)


def show_all_triggers(bot, update):
    if update.message.date < INIT_TIMESTAMP:
        return
    if update.message.from_user.id not in get_group_admin_ids(bot, update.message.chat_id):
        update.message.reply_text('Admin only')
        return
    if update.message.chat_id in TRIGGERS.keys():
        update.message.reply_text(
            text='Triggers in chat %s:\n%s' % (update.message.chat_id, '\n'.join(TRIGGERS[update.message.chat_id])),
            quote=False)
    else:
        update.message.reply_text('No trigger to show in this chat')


def show_recent_edits(bot, update):
    if update.message.date < INIT_TIMESTAMP:
        return
    result = db.select_edited_message(update.message.chat_id, 3)
    if result:
        lines = []
        for item in result:
            if item[4]:  # edited message
                text = '<b>[EDITED]</b>'
            else:
                text = '<b>[ORI]</b>'
            lines.append(text + '%s %s(%s): %s' % tuple(item[:4]))
        update.message.reply_text('\n'.join(lines), parse_mode='HTML')
    else:
        update.message.reply_text('No edits!')


def show_help(bot, update):
    if update.message.date < INIT_TIMESTAMP:
        return
    if update.message.from_user.id in BAN_IDS:
        return
    text = 'Available commands:\n' \
           'User:\n' \
           '/add <trigger_1>|<trigger_2>|...|<trigger_n>@<text_1>|<text_2>|...|<text_n>\n' \
           '/del <trigger_1>|<trigger_2>|...|<trigger_n>@<text_1>|<text_2>|...|<text_n>\n' \
           '/list <trigger>\n' \
           '/edits  //show recently edited message\n' \
           '/search <keyword1> <keyword2> ... \n' \
           '/stats\n' \
           '/echo <text> \n' \
           '/help\n' \
           'Admin only: \n' \
           '/merge trigger1=>trigger2 //move all text of trigger1 to trigger2 \n' \
           '/clear <trigger> \n' \
           '/triggers //list all triggers in this chat'
    update.message.reply_text(text)


def search(bot, update):
    if update.message.date < INIT_TIMESTAMP:
        return
    try:
        keywords = update.message.text.split(' ', 1)[1].split(' ')
    except IndexError:
        return
    result = db.search_text(keywords, update.message.chat_id, limit=5)
    if result:
        lines = []
        result.sort(key=lambda r: r[2], reverse=True)  # sort results by time (descending)
        for text in result:
            text = list(text)
            if len(text[3].split('\n')) > 3:  # limit result display to 3 lines
                text[3] = '\n'.join(text[3].split('\n')[0:3] + ['...'])
            lines.append('%s %s(%s): %s' % tuple(text))
        update.message.reply_text('\n'.join(lines))
    else:
        update.message.reply_text('Not found!')


def stats(bot, update):
    if update.message.date < INIT_TIMESTAMP:
        return
    if update.message.from_user.id in BAN_IDS:
        return
    result = db.query_chat_stats(update.message.chat_id)
    if not result:
        update.message.reply_text('No stats to show')
    else:
        lines = []
        for user in result:
            lines.append('%s %s (%d) => %d' % user)
        update.message.reply_text('\n'.join(lines), quote=False)


def echo(bot, update):
    if update.message.date < INIT_TIMESTAMP:
        return
    if update.message.from_user.id in BAN_IDS:
        return
    try:
        update.message.reply_text(update.message.text.split(' ', 1)[1])
    except IndexError:
        pass


def main():
    config = {}
    try:
        with open('config.json') as f:
            config = json.load(f)
    except json.JSONDecodeError or FileNotFoundError:
        print('Bad config!!!')
        exit(1)

    global BAN_IDS
    BAN_IDS = config['ban_id']
    db.__init__(config['db_path'])
    if sys.argv.get(1) == '--setup':
        db.setup()
    logging.basicConfig(level=config['debug_level'])
    update_trigger_list()

    updater = Updater(token=config['bot_token'])
    updater.dispatcher.add_handler(MessageHandler(Filters.all, process_chat_message, allow_edited=True), group=-1)
    # text logger & counter & user info update & recent edits

    updater.dispatcher.add_handler(CommandHandler('add', add))
    updater.dispatcher.add_handler(CommandHandler('del', delete))
    updater.dispatcher.add_handler(CommandHandler('list', list_text))
    updater.dispatcher.add_handler(CommandHandler('edits', show_recent_edits))
    updater.dispatcher.add_handler(CommandHandler('search', search))
    updater.dispatcher.add_handler(CommandHandler('stats', stats))
    updater.dispatcher.add_handler(CommandHandler('echo', echo))
    updater.dispatcher.add_handler(CommandHandler('help', show_help))
    updater.dispatcher.add_handler(CommandHandler('triggers', show_all_triggers))
    updater.dispatcher.add_handler(CommandHandler('merge', merge))
    updater.dispatcher.add_handler(CommandHandler('clear', clear))

    updater.dispatcher.add_handler(MessageHandler(Filters.text, process_trigger))
    # process text triggers

    updater.start_polling()


if __name__ == '__main__':
    main()
