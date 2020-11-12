import os
import re
import copy
import asyncio
import gspread
import objects
import requests
from time import sleep
import concurrent.futures
from aiogram import types
from bs4 import BeautifulSoup
from datetime import datetime
from aiogram.utils import executor
from objects import printer, time_now
from aiogram.dispatcher import Dispatcher
from requests_futures.sessions import FuturesSession

stamp1 = time_now()
search_array = {'Лот #': r'Лот #(\d+) : (.*)',
                'Качество: ': 'Качество: (.*)',
                'Состояние: ': 'Состояние: (.*)',
                'Модификаторы:': 'Modifiers:',
                'Продавец: ': 'Продавец: (.*)',
                'Цена: ': r'Текущая .*: (\d+)',
                'cw3': r'Цена: (\d+)',
                'Покупатель: ': 'Покупатель: (.+)',
                'Срок: ': r'Срок: (\d{2}) (.*) 10(\d{2}) (\d{2}):(\d{2})',
                'Статус: ': 'Статус: (.+)'}
db = {}
limiter = 1
update_id = 1
idMe = 396978030
last_requested = stamp1
idChannel = -1001319223716
objects.environmental_files(python=True)
# ====================================================================================


def start_db_creation():
    global db, limiter
    data1 = gspread.service_account('auction1.json').open('Action-Auction').worksheet('main')
    values = data1.col_values(1)
    cell_list = data1.range('A1:D' + str(len(values)))
    session = FuturesSession()
    values = int(values[0])
    counter = 1
    au_id = 0
    for i in cell_list:
        if i.row > 2:
            if i.col == 1:
                au_id = int(i.value)
                db[au_id] = {'update_id': 0,
                             'action': 'None',
                             '@cw3auction': [],
                             '@chatwars3': []}
            if i.col == 2:
                db[au_id]['@cw3auction'] = [int(i.value), 0, 'Запуск']
    while len(db) > counter:
        futures = []
        update_array = []
        if limiter >= 300:
            limiter = 1
        for i in db:
            if db[i]['update_id'] + 1 == update_id and limiter <= 300:
                db[i]['update_id'] = update_id
                update_array.append(i)
                limiter += 1
                counter += 1
        for i in update_array:
            url = 'https://t.me/cw3auction/' + str(db[i]['@cw3auction'][0]) + '?embed=1'
            futures.append(session.get(url))
        for future in concurrent.futures.as_completed(futures):
            result = former(future.result().content)
            last_time_request()
            if result[0] != 'False':
                for i in db:
                    if db[i]['@cw3auction'][0] == result[0]:
                        db[i]['@cw3auction'] = result
                        break
        if limiter >= 300:
            sleep(60)
    return values, data1


au_post, data = start_db_creation()
Auth = objects.AuthCentre(os.environ['TOKEN'])
bot = Auth.start_main_bot('async')
dispatcher = Dispatcher(bot)
executive = Auth.async_exec
Auth.start_message(stamp1)
# ====================================================================================


def last_time_request():
    global last_requested
    last_requested = time_now()


def time_mash(stamp, lang=None):
    day = 0
    text = ''
    if lang is None:
        lang = {'day': 'д. ', 'hour': 'ч. ', 'min': ' мин.'}
    seconds = stamp - time_now()
    hours = int(seconds / (60 * 60))
    if hours > 24:
        day = int(hours / 24)
        hours -= day * 24
        text += str(day) + lang['day'] + str(hours) + lang['hour']
    elif hours > 0:
        text += str(hours) + lang['hour']
    elif hours < 0:
        hours = 0
    minutes = int((seconds / 60) - (day * 24 * 60) - (hours * 60))
    response = objects.log_time(stamp, tag=objects.italic, form='au_normal')
    if minutes >= 0:
        response += '\nОсталось:' + objects.italic('  ~ ' + text + str(minutes) + lang['min'])
    return response


def form_mash(au_id, lot):
    from timer import timer
    text = ''
    price = 0
    lot_id = ''
    channel = 'au'
    status = 'None'
    modifiers = 'None'
    stamp_now = time_now() - 24 * 60 * 60
    stamp = stamp_now - 10
    for g in lot.split('\n'):
        for i in search_array:
            search = re.search(search_array.get(i), g)
            if search:
                if i == 'Лот #':
                    text += i + search.group(1) + ' : ' + objects.bold(search.group(2)) + '\n'
                    lot_id = search.group(1)
                elif i == 'Модификаторы:':
                    text += i + '\n{0}'
                    modifiers = ''
                elif i == 'Цена: ':
                    text += i + search.group(1) + ' 👝\n'
                    price = int(search.group(1))
                elif i == 'cw3':
                    price = int(search.group(1))
                    channel = i
                elif i == 'Срок: ':
                    stamp = timer(search)
                    text += i + str(time_mash(stamp)) + '\n'
                elif i == 'Статус: ':
                    text += i
                    if search.group(1) in ['Cancelled', 'Failed', 'Отменен']:
                        status = 'Отменен'
                    elif search.group(1) == '#активен':
                        status = '#активен'
                    elif search.group(1) == '#active':
                        if stamp < stamp_now:
                            status = 'Закончился'
                        else:
                            status = '#активен'
                    else:
                        status = 'Закончился'
                    text += status
                    if status == '#активен':
                        text += '\n\n/bet_{1} /l_{1}'
                else:
                    if search.group(1) == 'None':
                        text += i + 'Нет\n'
                    else:
                        text += i + search.group(1) + '\n'
        if modifiers != 'None' and g.startswith('  '):
            modifiers += g + '\n'
    if channel == 'au':
        text = text.format(modifiers, lot_id)
        return [au_id, text, price, status]
    else:
        return [au_id, price, status]


def former(content):
    soup = BeautifulSoup(content, 'html.parser')
    is_post_not_exist = str(soup.find('div', class_='tgme_widget_message_error'))
    if str(is_post_not_exist) == 'None':
        lot_raw = str(soup.find('div', class_='tgme_widget_message_text js-message_text')).replace('<br/>', '\n')
        get_au_id = soup.find('div', class_='tgme_widget_message_link')
        if get_au_id:
            au_id = int(re.sub('t.me/.*?/', '', get_au_id.get_text()))
            lot = BeautifulSoup(lot_raw, 'html.parser').get_text()
            goo = form_mash(au_id, lot)
        else:
            goo = ['False']
    else:
        goo = ['False']
    return goo


def google(action, option):
    global db, data
    if action == 'col_values':
        try:
            values = data.col_values(option)
        except IndexError and Exception:
            data = gspread.service_account('auction1.json').open('Action-Auction').worksheet('main')
            values = data.col_values(option)
        sleep(1)
    elif action == 'insert':
        try:
            values = data.insert_row(option, 3)
        except IndexError and Exception:
            data = gspread.service_account('auction1.json').open('Action-Auction').worksheet('main')
            values = data.insert_row(option, 3)
        sleep(1)
    elif action == 'update_cell':
        try:
            values = data.update_cell(1, 1, option)
        except IndexError and Exception:
            data = gspread.service_account('auction1.json').open('Action-Auction').worksheet('main')
            values = data.update_cell(1, 1, option)
        sleep(1)
    elif action == 'delete':
        try:
            values = data.delete_row(option)
        except IndexError and Exception:
            data = gspread.service_account('auction1.json').open('Action-Auction').worksheet('main')
            values = data.delete_row(option)
        sleep(1)
    else:
        values = None
    sleep(1)
    return values


@dispatcher.message_handler()
async def repeat_all_messages(message: types.Message):
    try:
        if message['chat']['id'] == idMe:
            if message['text'].startswith('/log'):
                doc = open('log.txt', 'rb')
                await bot.send_document(message['chat']['id'], doc)
                doc.close()
            if message['text'].startswith('/db'):
                temp_db = copy.copy(db)
                for i in temp_db:
                    print(str(i) + ': ' + str(db.get(i)))
    except IndexError and Exception:
        await executive(str(message))


async def detector():
    global db, au_post
    while True:
        try:
            await asyncio.sleep(0.3)
            array = former(requests.get('https://t.me/chatwars3/' + str(au_post) + '?embed=1').text)
            if array[0] != 'False':
                try:
                    post = await bot.send_message(idChannel, array[1], parse_mode='HTML')
                    db[au_post] = {
                        'update_id': update_id,
                        'action': 'Add',
                        '@cw3auction': form_mash(post['message_id'], post['text']),
                        '@chatwars3': array}
                    au_post += 1
                    google('update_cell', au_post)
                    printer('запостил новый лот')
                except IndexError and Exception as error:
                    printer(error)
                await asyncio.sleep(1)
        except IndexError and Exception:
            await executive()


async def lot_updater():
    global db, limiter, update_id
    while True:
        try:
            await asyncio.sleep(1)
            printer('начало')
            g_actives = google('col_values', 1)
            stamp2 = datetime.now().timestamp()
            session = FuturesSession()
            temp_db = copy.copy(db)
            update_array = []
            update_id += 1
            futures = []

            for i in temp_db:
                lot = db.get(i)
                if lot['action'] == 'Add':
                    google('insert', [i, lot['@cw3auction'][0]])
                    g_actives.insert(2, str(i))
                    db[i]['action'] = 'None'

            for i in temp_db:
                if db[i]['action'] != 'deleted':
                    lot = db.get(i)
                    if lot['update_id'] + 1 < update_id:
                        update_array = []
                        update_id -= 1
                        limiter = 1
                    if lot['update_id'] + 1 == update_id and limiter <= 300:
                        db[i]['update_id'] = update_id
                        update_array.append(i)
                        limiter += 1

            for i in update_array:
                url = 'https://t.me/chatwars3/' + str(i) + '?embed=1'
                futures.append(session.get(url))

            for future in concurrent.futures.as_completed(futures):
                result = former(future.result().content)
                last_time_request()
                if result[0] != 'False':
                    db[result[0]]['@chatwars3'] = result
                    lot_cw3 = db[result[0]]['@cw3auction']
                    if result[2] != lot_cw3[1]:
                        db[result[0]]['action'] = 'Update'
                    if result[3] != '#активен':
                        db[result[0]]['action'] = 'Delete'

            for i in temp_db:
                lot = db.get(i)
                if lot['action'] in ['Update', 'Delete']:
                    try:
                        post = await bot.edit_message_text(lot['@chatwars3'][1], idChannel,
                                                           lot['@cw3auction'][0], parse_mode='HTML')
                        print_text = 'Пост обновлен'
                        if lot['action'] == 'Update':
                            db[i]['action'] = 'None'
                            db[i]['@cw3auction'] = form_mash(post['message_id'], post['text'])
                        else:
                            google('delete', g_actives.index(str(i)) + 1)
                            g_actives.pop(g_actives.index(str(i)))
                            db[i]['action'] = 'deleted'
                            print_text += ' (закончился) и удален из обновлений'
                    except IndexError and Exception as e:
                        print_text = 'Пост не изменился'
                        search = re.search('exactly the same as a current content', str(e))
                        if search:
                            print_text += ', потому что точно такой же'
                            if lot['action'] == 'Update':
                                db[i]['@cw3auction'][1] = db[i]['@chatwars3'][2]
                                db[i]['@cw3auction'][2] = db[i]['@chatwars3'][3]
                                db[i]['action'] = 'None'
                            else:
                                print_text += ', а еще он закончился и удален из обновлений'
                                google('delete', g_actives.index(str(i)) + 1)
                                g_actives.pop(g_actives.index(str(i)))
                                db[i]['action'] = 'deleted'
                        else:
                            print_text += ' ' + str(e)
                    printer(str(i) + '-' + str(lot['@cw3auction'][0]) + ' ' + print_text)
            limiter = 1
            printer('конец ' + str(datetime.now().timestamp() - stamp2))
            delay = 60 - (time_now() - last_requested)
            if delay >= 0:
                await asyncio.sleep(delay)
        except IndexError and Exception:
            await executive()


async def supporter():
    while True:
        try:
            await asyncio.sleep(10)
            stamp2 = datetime.now().timestamp()
            temp_db = copy.copy(db)
            for i in temp_db:
                lot = db.get(i)
                try:
                    await bot.edit_message_text(lot['@chatwars3'][1], idChannel,
                                                lot['@cw3auction'][0], parse_mode='HTML')
                    print_text = 'Пост обновлен'
                except IndexError and Exception as e:
                    print_text = 'Пост не изменился'
                    search = re.search('exactly the same as a current content', str(e))
                    if search:
                        print_text += ', потому что точно такой же'
                    else:
                        print_text += ' ' + str(e)
                    await asyncio.sleep(10)
                printer(str(i) + '-' + str(lot['@cw3auction'][0]) + ' ' + print_text)
                await asyncio.sleep(10)
            printer('конец ' + str(datetime.now().timestamp() - stamp2))
        except IndexError and Exception:
            await executive()


if __name__ == '__main__':
    gain = [detector, lot_updater, supporter]
    for thread_element in gain:
        dispatcher.loop.create_task(thread_element())
    executor.start_polling(dispatcher)
