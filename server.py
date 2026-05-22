# server.py — WebSocket + HTTP сервер на Python

import asyncio
import websockets
import json
import uuid
import html
import os
from aiohttp import web
from datetime import datetime

import database as db

# =============================================
# НАСТРОЙКИ
# =============================================

PORT = int(os.environ.get('PORT', 8080))
WS_PORT = int(os.environ.get('WS_PORT', 8765))
HOST = '0.0.0.0'

# =============================================
# ХРАНИЛИЩЕ КЛИЕНТОВ
# =============================================

# clients: session_id -> { websocket, ip, answers, branch, connected_at }
clients = {}

# Rate limiting: ip -> { count, start_time }
rate_limits = {}

# =============================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# =============================================

def escape(text):
    """Защита от XSS"""
    if not isinstance(text, str):
        return ''
    return html.escape(str(text)[:1000])


def check_rate_limit(ip):
    """Защита от спама — не более 60 сообщений в минуту"""
    now = datetime.now().timestamp()
    window = 60
    max_requests = 60

    if ip not in rate_limits:
        rate_limits[ip] = {'count': 1, 'start': now}
        return True

    data = rate_limits[ip]

    if now - data['start'] > window:
        rate_limits[ip] = {'count': 1, 'start': now}
        return True

    if data['count'] >= max_requests:
        return False

    data['count'] += 1
    return True


async def send_json(websocket, data):
    """Безопасная отправка JSON"""
    try:
        await websocket.send(json.dumps(data, ensure_ascii=False))
    except Exception as e:
        print(f'❌ Ошибка отправки: {e}')


# =============================================
# WEBSOCKET ОБРАБОТЧИК
# =============================================

async def handle_client(websocket):
    """Обработка каждого WebSocket клиента"""

    # Получаем IP клиента
    try:
        ip = websocket.remote_address[0] if websocket.remote_address else 'unknown'
    except Exception:
        ip = 'unknown'

    # Проверяем rate limit
    if not check_rate_limit(ip):
        await websocket.close(1008, 'Too many requests')
        print(f'🚫 Rate limit: {ip}')
        return

    # Генерируем ID сессии
    session_id = str(uuid.uuid4())

    # Регистрируем клиента
    clients[session_id] = {
        'websocket': websocket,
        'ip': ip,
        'answers': {},
        'branch': '',
        'connected_at': datetime.now().isoformat()
    }

    # Создаём сессию в БД
    await db.create_session(session_id, ip)

    print(f'✅ Новый клиент: {session_id[:8]}... (IP: {ip})')
    print(f'👥 Активных клиентов: {len(clients)}')

    # Приветствие
    await send_json(websocket, {
        'type': 'connected',
        'sessionId': session_id,
        'message': 'Соединение установлено'
    })

    try:
        # Слушаем сообщения
        async for raw_message in websocket:

            # Парсим JSON
            try:
                msg = json.loads(raw_message)
            except json.JSONDecodeError:
                print('❌ Невалидный JSON')
                continue

            msg_type = msg.get('type', '')
            client = clients.get(session_id)
            if not client:
                break

            print(f'📨 [{session_id[:8]}] Тип: {msg_type}')

            # =========================================
            # ОБРАБОТКА ОТВЕТА ПОЛЬЗОВАТЕЛЯ
            # =========================================
            if msg_type == 'user_answer':
                answer = escape(msg.get('answer', ''))
                step   = escape(msg.get('step', ''))

                # Сохраняем ответ
                client['answers'][step] = answer

                # Определяем ветку диалога
                if step == 'start':
                    client['branch'] = answer

                # Сохраняем в БД
                await db.save_message(session_id, 'user', answer, step)

                # Подтверждение
                await send_json(websocket, {
                    'type': 'ack',
                    'status': 'received',
                    'step': step
                })

            # =========================================
            # ОБРАБОТКА ОТПРАВКИ ФОРМЫ
            # =========================================
            elif msg_type == 'submit_form':
                name    = escape(msg.get('name', '')).strip()
                phone   = escape(msg.get('phone', '')).strip()
                comment = escape(msg.get('comment', '')).strip()

                # Валидация имени
                if len(name) < 2:
                    await send_json(websocket, {
                        'type': 'form_result',
                        'success': False,
                        'error': 'Имя слишком короткое'
                    })
                    continue

                # Валидация телефона
                phone_digits = ''.join(filter(str.isdigit, phone))
                if len(phone_digits) < 10:
                    await send_json(websocket, {
                        'type': 'form_result',
                        'success': False,
                        'error': 'Неверный номер телефона'
                    })
                    continue

                # Сохраняем заявку в БД
                app_id = await db.save_application(
                    name    = name,
                    phone   = phone,
                    comment = comment,
                    answers = client['answers'],
                    branch  = client['branch'],
                    ip      = ip
                )

                # Сохраняем финальное сообщение
                await db.save_message(
                    session_id, 'user',
                    f'Заявка: {name}, {phone}',
                    'final_form'
                )

                # Завершаем сессию
                await db.finish_session(session_id)

                print(f'🎉 Новая заявка #{app_id}: {name} | {phone}')

                # Отправляем успех
                await send_json(websocket, {
                    'type': 'form_result',
                    'success': True,
                    'applicationId': app_id
                })

            # =========================================
            # PING
            # =========================================
            elif msg_type == 'ping':
                await send_json(websocket, {
                    'type': 'pong',
                    'timestamp': datetime.now().isoformat()
                })

    except websockets.exceptions.ConnectionClosed:
        pass
    except Exception as e:
        print(f'❌ Ошибка [{session_id[:8]}]: {e}')
    finally:
        # Удаляем клиента
        if session_id in clients:
            del clients[session_id]
        print(f'👋 Клиент отключился: {session_id[:8]}')
        print(f'👥 Активных клиентов: {len(clients)}')


# =============================================
# HTTP API ДЛЯ АДМИНКИ
# =============================================

async def api_applications(request):
    """Все заявки"""
    data = await db.get_all_applications()
    return web.json_response(data)


async def api_application_detail(request):
    """Одна заявка"""
    app_id = int(request.match_info['id'])
    data = await db.get_application_by_id(app_id)
    if not data:
        return web.json_response({'error': 'Не найдено'}, status=404)
    return web.json_response(data)


async def api_stats(request):
    """Статистика"""
    data = await db.get_stats()
    return web.json_response(data)


async def api_sessions(request):
    """Все сессии"""
    data = await db.get_all_sessions()
    return web.json_response(data)


async def api_online(request):
    """Активные клиенты"""
    online = []
    for sid, c in clients.items():
        online.append({
            'sessionId': sid[:8] + '...',
            'ip': c['ip'],
            'connectedAt': c['connected_at'],
            'branch': c['branch']
        })
    return web.json_response({
        'count': len(clients),
        'clients': online
    })


async def serve_file(request):
    """Отдаём HTML файлы"""
    filename = request.match_info.get('filename', 'index.html')

    # Безопасность — только .html файлы
    if not filename.endswith('.html'):
        raise web.HTTPForbidden()

    filepath = os.path.join(os.path.dirname(__file__), filename)

    if not os.path.exists(filepath):
        raise web.HTTPNotFound()

    return web.FileResponse(filepath)


# =============================================
# CORS MIDDLEWARE
# =============================================

@web.middleware
async def cors_middleware(request, handler):
    """Разрешаем запросы с любых доменов"""
    if request.method == 'OPTIONS':
        response = web.Response()
    else:
        response = await handler(request)

    response.headers['Access-Control-Allow-Origin']  = '*'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
    return response


# =============================================
# ЗАПУСК СЕРВЕРОВ
# =============================================

async def main():
    """Главная функция запуска"""

    # Инициализируем БД
    await db.init_db()

    # ===== HTTP СЕРВЕР (aiohttp) =====
    app = web.Application(middlewares=[cors_middleware])

    # API маршруты
    app.router.add_get('/api/applications',      api_applications)
    app.router.add_get('/api/applications/{id}', api_application_detail)
    app.router.add_get('/api/stats',             api_stats)
    app.router.add_get('/api/sessions',          api_sessions)
    app.router.add_get('/api/online',            api_online)

    # HTML файлы
    app.router.add_get('/',             lambda r: serve_file(
        type('R', (), {'match_info': {'filename': 'index.html'}})()
    ))
    app.router.add_get('/{filename}',   serve_file)

    # Статические файлы
    static_path = os.path.join(os.path.dirname(__file__), 'static')
    if os.path.exists(static_path):
        app.router.add_static('/static/', static_path)

    # Запускаем HTTP
    runner = web.AppRunner(app)
    await runner.setup()
    http_site = web.TCPSite(runner, HOST, PORT)
    await http_site.start()

    # ===== WEBSOCKET СЕРВЕР =====
    ws_server = await websockets.serve(
        handle_client,
        HOST,
        WS_PORT
    )

    print('')
    print('🚀 ================================')
    print(f'🐍 Python WebSocket Сервер')
    print(f'🌐 Сайт:    http://localhost:{PORT}')
    print(f'🔧 Админка: http://localhost:{PORT}/admin.html')
    print(f'📊 API:     http://localhost:{PORT}/api/stats')
    print(f'🔌 WS:      ws://localhost:{WS_PORT}')
    print('🚀 ================================')
    print('')
    print('Нажми Ctrl+C для остановки')
    print('')

    # Работаем вечно
    await asyncio.Future()


if __name__ == '__main__':
    asyncio.run(main())