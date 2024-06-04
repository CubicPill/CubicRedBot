import datetime
import json
import logging
import random
import sys

import pytz

timezone = pytz.timezone('Asia/Shanghai')
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

import bot_db as db

TRIGGERS = {}
BAN_IDS = []
INIT_TIMESTAMP = datetime.datetime.utcnow().replace(tzinfo=timezone)

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


async def get_group_admin_ids(bot, chat_id):
    result = await bot.get_chat_administrators(chat_id)
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


async def add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        content = update.message.text.split(' ', 1)[1].split('@', 1)
        triggers = content[0].lower().split('|')
        texts = content[1].split('|')
    except IndexError:
        await update.message.reply_text('没东西 add 个大头鬼啦')
        return
    for text in texts:
        if not len(text) <= 140:
            await update.message.reply_text('2<=len(trigger)<=140, len(text)<=140')
            return
    for trigger in triggers:
        if not 2 <= len(trigger) <= 140:
            await update.message.reply_text('2<=len(trigger)<=140, len(text)<=140')
            return

    result = db.add_trigger_text(triggers, texts, update.message.chat_id)
    if result != db.SUCCESS:
        lines = []
        for item in result:
            lines.append('- %s@%s already exists' % item)
        await update.message.reply_text('Result:\n' + '\n'.join(lines))

    await update.message.reply_text('done!')
    update_trigger_list(update.message.chat_id)


async def delete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        content = update.message.text.split(' ', 1)[1].split('@', 1)
        triggers = content[0].lower().split('|')
        texts = content[1].split('|')
    except IndexError:
        return
    db.delete_trigger_text(triggers, texts, update.message.chat_id)
    await update.message.reply_text('deleted!')
    update_trigger_list(update.message.chat_id)


async def list_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.date < INIT_TIMESTAMP:
        return
    try:
        trigger = update.message.text.split(' ', 1)[1].lower()
    except IndexError:
        return

    result = db.query_all_text_of_trigger(trigger, update.message.chat_id)
    if result:
        await update.message.reply_text('\n'.join(result))
    else:
        await update.message.reply_text('Empty list!')


async def merge(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id not in get_group_admin_ids(update.get_bot(), update.message.chat_id):
        await update.message.reply_text('Admin only')
        return
    try:
        content = update.message.text.split(' ', 1)[1].split('=>')
        trigger_from = content[0].lower()
        trigger_to = content[1].lower()
    except IndexError:
        await update.message.reply_text('Missing arguments!')
        return
    db.merge_trigger(trigger_from, trigger_to, update.message.chat_id)
    await update.message.reply_text('merge done!')


async def clear(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id not in get_group_admin_ids(update.get_bot(), update.message.chat_id):
        await update.message.reply_text('Admin only')
        return
    try:
        trigger = update.message.text.split(' ', 1)[1].lower()
    except IndexError:
        return
    db.clear_trigger(trigger, update.message.chat_id)
    await update.message.reply_text('cleared!')
    update_trigger_list(update.message.chat_id)


async def process_trigger(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.date < INIT_TIMESTAMP:
        return
    if update.message.from_user.id in BAN_IDS:
        return
    if TRIGGERS.get(update.message.chat_id):
        matched_triggers = []

        for trigger in TRIGGERS.get(update.message.chat_id):
            if trigger in await update.message.text.lower():
                matched_triggers.append(trigger)
        if matched_triggers:
            await update.message.reply_text(
                    db.query_random_trigger_text(random.choice(matched_triggers), update.message.chat_id))


async def process_chat_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.edited_message and update.edited_message.text and not update.edited_message.text.startswith(
            '/'):  # process edited message
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

        if update.message.new_chat_members:  # entering group trigger
            await update.message.reply_text('新人请发红包,支付宝QQ微信都可以')
        if update.message.left_chat_member:  # left group trigger
            await update.message.reply_text('@%s 跑了!' % update.message.left_chat_member.username)


async def show_all_triggers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.date < INIT_TIMESTAMP:
        return
    if update.message.from_user.id != update.message.chat_id:  # not in private chat
        if update.message.from_user.id not in get_group_admin_ids(update.get_bot(), update.message.chat_id):
            await update.message.reply_text('Admin only')
            return
    if update.message.chat_id in TRIGGERS.keys():
        await update.message.reply_text(
                text='Triggers in chat %s:\n%s' % (update.message.chat_id, '\n'.join(TRIGGERS[update.message.chat_id])),
                quote=False)
    else:
        await update.message.reply_text('No trigger to show in this chat')


async def show_recent_edits(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
        await update.message.reply_text('\n'.join(lines), parse_mode='HTML')
    else:
        await update.message.reply_text('No edits!')


async def show_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
    await update.message.reply_text(text)


async def search(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
        await update.message.reply_text('\n'.join(lines))
    else:
        await update.message.reply_text('Not found!')


async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.date < INIT_TIMESTAMP:
        return
    if update.message.from_user.id in BAN_IDS:
        return
    result = db.query_chat_stats(update.message.chat_id)
    if not result:
        await update.message.reply_text('No stats to show')
    else:
        lines = []
        for user in result:
            fn, ln, uid, count = user
            full_name = '{} {}'.format(fn, ln) if ln else fn
            lines.append('%s (%d) => %d' % (full_name, uid, count))
        content = '\n'.join(lines)

        if len(content) <= 4000:
            await update.message.reply_text(content, quote=False)
        else:  # message too long, need to split
            i = 0
            it = 0
            while i < len(content):
                it += 1
                if it >= 20: break
                for j in range(min(4000, len(content) - i - 1), 0, -1):
                    if content[i + j] == '\n' or i + j == len(content) - 1:
                        await update.message.reply_text(content[i:i + j + 1], quote=False)
                        i = i + j + 1
                        break


async def echo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.date < INIT_TIMESTAMP:
        return
    if update.message.from_user.id in BAN_IDS:
        return
    try:
        await update.message.reply_text(update.message.text.split(' ', 1)[1])
    except IndexError:
        pass


def gamble(bot, update):
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
    if len(sys.argv) >= 2:
        if sys.argv[1] == '--setup':
            db.setup()
    logging.basicConfig(level=config['debug_level'])
    update_trigger_list()

    application = Application.builder().token(config['bot_token']).build()
    application.add_handler(MessageHandler(filters.ALL, process_chat_message), group=-1)
    # text logger & counter & user info update & recent edits

    application.add_handler(CommandHandler('add', add))
    application.add_handler(CommandHandler('del', delete))
    application.add_handler(CommandHandler('list', list_text))
    application.add_handler(CommandHandler('edits', show_recent_edits))
    application.add_handler(CommandHandler('search', search))
    application.add_handler(CommandHandler('stats', stats))
    application.add_handler(CommandHandler('echo', echo))
    application.add_handler(CommandHandler('help', show_help))
    application.add_handler(CommandHandler('triggers', show_all_triggers))
    application.add_handler(CommandHandler('merge', merge))
    application.add_handler(CommandHandler('clear', clear))

    application.add_handler(MessageHandler(filters.TEXT, process_trigger))
    # process text triggers

    application.run_polling()


if __name__ == '__main__':
    main()
