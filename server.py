import socketio
import aiohttp
from aiohttp import web
import os

sio = socketio.AsyncServer(async_mode='aiohttp', cors_allowed_origins='*')
app = web.Application()
sio.attach(app)

import asyncio

rooms = {}
user_to_room = {}
user_names = {}
disconnect_timers = {}

async def cleanup_room(room_id):
    await asyncio.sleep(12)  # 12-second grace period for host refresh
    if room_id in rooms:
        await sio.emit('broadcast', {'type': 'kicked', 'reason': 'The host has left the room.'}, room=room_id)
        del rooms[room_id]
    if room_id in disconnect_timers:
        del disconnect_timers[room_id]

async def index(request):
    return web.Response(text="Backend is running!")

app.router.add_get('/', index)

@sio.event
async def join_room(sid, data):
    room_id = data.get('roomId')
    is_host = data.get('isHost', False)
    username = data.get('username', 'Guest')
    user_names[sid] = username
    
    # Cancel pending cleanup if room is re-joined (e.g. host refresh)
    if room_id in disconnect_timers:
        disconnect_timers[room_id].cancel()
        del disconnect_timers[room_id]
    
    if room_id not in rooms:
        rooms[room_id] = {
            'host': sid,
            'currentVideoUrl': '',
            'videoQueue': [],
            'chatHistory': [],
            'hostOnlyVideo': True,
            'users': [sid]
        }
    else:
        room = rooms[room_id]
        if is_host:
            room['host'] = sid
        if sid not in room['users']:
            room['users'].append(sid)
    
    await sio.enter_room(sid, room_id)
    user_to_room[sid] = room_id
    
    room = rooms[room_id]
    
    # Notify everyone of the new user count
    await sio.emit('user-count', {'count': len(room['users'])}, room=room_id)
    
    # Notify others in the room that a user joined
    if not is_host:
        await sio.emit('broadcast', {'type': 'user-joined', 'name': username}, room=room_id, skip_sid=sid)
    
    return {
        'success': True,
        'currentVideoUrl': room['currentVideoUrl'],
        'videoQueue': room['videoQueue'],
        'chatHistory': room['chatHistory'],
        'hostOnlyVideo': room['hostOnlyVideo'],
    }

@sio.event
async def disconnect(sid):
    room_id = user_to_room.get(sid)
    username = user_names.pop(sid, 'Guest')
    if room_id and room_id in rooms:
        room = rooms[room_id]
        if sid in room['users']:
            room['users'].remove(sid)
            
        if room['host'] == sid:
            # Schedule a 12-second grace period before destroying room (allows host page refresh)
            if room_id in disconnect_timers:
                disconnect_timers[room_id].cancel()
            timer = asyncio.create_task(cleanup_room(room_id))
            disconnect_timers[room_id] = timer
        else:
            await sio.emit('user-count', {'count': len(room['users'])}, room=room_id)
            await sio.emit('broadcast', {'type': 'user-left', 'name': username}, room=room_id)
    
    if sid in user_to_room:
        del user_to_room[sid]

@sio.event
async def broadcast(sid, data):
    room_id = user_to_room.get(sid)
    print(f"Broadcast from {sid} in room {room_id}: {data.get('type')}")
    if room_id and room_id in rooms:
        # Update server state for late joiners
        if data.get('type') == 'video-sync':
            rooms[room_id]['currentVideoUrl'] = data.get('url')
        elif data.get('type') == 'queue-sync':
            rooms[room_id]['videoQueue'] = data.get('queue')
        elif data.get('type') == 'host-settings':
            rooms[room_id]['hostOnlyVideo'] = data.get('hostOnlyVideo')
        elif data.get('type') == 'structured-chat':
            rooms[room_id]['chatHistory'].append(data)
        elif data.get('type') == 'name-change':
            user_names[sid] = data.get('newName', 'Guest')
            
        await sio.emit('broadcast', data, room=room_id, skip_sid=sid)

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8000))
    web.run_app(app, host='0.0.0.0', port=port)
