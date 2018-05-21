import sqlite3
from functools import reduce

SUCCESS = 's'
NOT_FOUND = 'nf'
ALREADY_EXIST = 'ae'
ERROR = False
connection = None


def __init__(path):
    global connection
    connection = sqlite3.connect(path, check_same_thread=False)


def setup():
    sql_trigger = 'CREATE TABLE IF NOT EXISTS triggers (' \
                  'trigger VARCHAR(140),' \
                  'text VARCHAR(140),' \
                  'chat_id INTEGER,' \
                  'PRIMARY KEY(trigger,text,chat_id)' \
                  ')'

    sql_userinfo = 'CREATE TABLE IF NOT EXISTS userinfo (' \
                   'user_id INTEGER,' \
                   'chat_id INTEGER,' \
                   'first_name TEXT,' \
                   'last_name TEXT,' \
                   'count INTEGER DEFAULT 0,' \
                   'PRIMARY KEY(user_id,chat_id)' \
                   ')'

    sql_chats = 'CREATE TABLE IF NOT EXISTS chats (' \
                'update_id INTEGER,' \
                'message_id INTEGER,' \
                'user_id INTEGER,' \
                'chat_id INTEGER,' \
                'text TEXT,' \
                'time DATETIME,' \
                'edited BOOLEAN DEFAULT 0,' \
                'PRIMARY KEY(update_id)' \
                ')'
    connection.execute(sql_trigger)
    connection.execute(sql_userinfo)
    connection.execute(sql_chats)
    connection.commit()


def add_trigger_text(triggers, texts, chat_id):
    fails = []
    for trigger in triggers:
        for text in texts:
            try:
                connection.execute('INSERT INTO triggers (trigger,text,chat_id) VALUES (?,?,?)',
                                   (trigger, text, chat_id))
            except sqlite3.IntegrityError:
                fails.append((trigger, text))

    connection.commit()
    if fails:
        return fails
    else:
        return SUCCESS


def delete_trigger_text(triggers, texts, chat_id):
    for trigger in triggers:
        for text in texts:
            connection.execute('DELETE FROM triggers WHERE trigger=? AND text=? AND chat_id=?',
                               (trigger, text, chat_id))
    connection.commit()
    return SUCCESS


def merge_trigger(trigger_from, trigger_to, chat_id):
    fails = []
    cursor = connection.execute('SELECT text FROM triggers WHERE trigger=? AND chat_id=?', (trigger_from, chat_id))
    result = cursor.fetchall()

    for text in result:
        try:
            connection.execute('INSERT INTO triggers (trigger,text,chat_id) VALUES (?,?,?)',
                               (trigger_to, text[0], chat_id))
        except sqlite3.IntegrityError:
            fails.append((trigger_to, text))
    connection.commit()
    if fails:
        return fails
    else:
        return SUCCESS


def clear_trigger(trigger, chat_id):
    connection.execute('DELETE FROM triggers WHERE trigger=? AND chat_id=?', (trigger, chat_id))
    connection.commit()
    return SUCCESS


def query_random_trigger_text(trigger, chat_id):
    # return a text str
    cursor = connection.execute('SELECT text FROM triggers WHERE trigger=? AND chat_id=? ORDER BY RANDOM()',
                                (trigger, chat_id))
    return cursor.fetchone()[0]


def query_all_text_of_trigger(trigger, chat_id):
    # returns a list of text of a trigger in a specific chat
    cursor = connection.execute('SELECT text FROM triggers WHERE trigger=? AND chat_id=?', (trigger, chat_id))
    return [r[0] for r in cursor.fetchall()]


def query_all_triggers_in_chat(chat_id):
    # returns a list of triggers in a chat
    cursor = connection.execute('SELECT DISTINCT trigger FROM triggers WHERE chat_id=?', (chat_id,))
    return [r[0] for r in cursor.fetchall()]


def query_all_triggers():
    # returns a list of triggers in all chats
    cursor = connection.execute('SELECT DISTINCT trigger,chat_id FROM triggers')
    return cursor.fetchall()


def update_user_info(user_id, chat_id, first_name, last_name):
    connection.execute('INSERT OR IGNORE INTO userinfo (first_name,last_name,user_id,chat_id) VALUES (?,?,?,?)',
                       (first_name, last_name, user_id, chat_id))
    connection.execute('UPDATE userinfo SET first_name=?,last_name=? WHERE user_id=? AND chat_id=?',
                       (first_name, last_name, user_id, chat_id))
    connection.commit()


def update_count(user_id, chat_id):
    # count++
    connection.execute('UPDATE userinfo SET count=count+1 WHERE user_id=? AND chat_id=?', (user_id, chat_id))
    connection.commit()


def query_chat_stats(chat_id):
    # return all user's fn,ln,uid,count for a chat
    result = connection.execute(
        'SELECT first_name,last_name,user_id,count FROM userinfo WHERE chat_id=? ORDER BY count DESC',
        (chat_id,))
    return result.fetchall()


def log_message(message_id, text, chat_id, user_id, time, edited=False):
    connection.execute('INSERT INTO chats (message_id,text,chat_id,user_id,time,edited) VALUES (?,?,?,?,?,?)',
                       (message_id, text, chat_id, user_id, time, edited))
    connection.commit()


def search_text(keywords, chat_id, limit=5):
    result = []
    query_result = []
    for keyword in keywords:
        cursor = connection.execute('SELECT update_id FROM chats WHERE chat_id=? AND text LIKE ?',
                                    (chat_id, '%' + keyword + '%'))
        query_result.append(cursor.fetchall())
    matched_ids = [r[0] for r in reduce(lambda x, y: set(x) & set(y), query_result)]
    for uid in matched_ids:
        cursor = connection.execute(
            'SELECT userinfo.first_name,userinfo.last_name,chats.time,chats.text FROM chats '
            'LEFT JOIN userinfo ON chats.user_id=userinfo.user_id AND chats.chat_id=userinfo.chat_id '
            'WHERE chats.update_id=? AND chats.chat_id=? ORDER BY chats.time DESC', (uid, chat_id))
        result.extend(cursor.fetchall())
        if len(result) >= limit:
            break
    return result


def select_edited_message(chat_id, limit=3):
    result = []
    cursor = connection.execute('SELECT message_id FROM chats WHERE chat_id=? AND edited=1 ORDER BY time DESC LIMIT ?',
                                (chat_id, limit))

    edited_message_ids = list(set([r[0] for r in cursor.fetchall()]))
    for mid in edited_message_ids:
        cursor = connection.execute(
            'SELECT userinfo.first_name,userinfo.last_name,chats.time,chats.text,chats.edited FROM chats '
            'LEFT JOIN userinfo ON chats.user_id=userinfo.user_id AND chats.chat_id=userinfo.chat_id '
            'WHERE chats.message_id=? AND chats.chat_id=? ORDER BY chats.time', (mid, chat_id))
        result.extend(cursor.fetchall())
    return result
