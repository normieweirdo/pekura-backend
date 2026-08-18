import socketio
import aiohttp
from aiohttp import web
import os

sio = socketio.AsyncServer(async_mode='aiohttp', cors_allowed_origins='*')
app = web.Application()
sio.attach(app)

rooms = {}
user_to_room = {}

async def index(request):
    return web.Response(text="Backend is running!")

app.router.add_get('/', index)

@sio.event
async def join_room(sid, data):
    room_id = data.get('roomId')
    is_host = data.get('isHost', False)
    
    if is_host:
        rooms[room_id] = {
            'host': sid,
            'currentVideoUrl': '',
            'videoQueue': [],
            'chatHistory': [],
            'hostOnlyVideo': True,
            'users': [sid]
        }
    else:
        if room_id not in rooms:
            return {'success': False, 'error': 'Room not found or host left.'}
        rooms[room_id]['users'].append(sid)
    
    await sio.enter_room(sid, room_id)
    user_to_room[sid] = room_id
    
    room = rooms[room_id]
    
    # Notify everyone of the new user count
    await sio.emit('user-count', {'count': len(room['users'])}, room=room_id)
    
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
    if room_id and room_id in rooms:
        room = rooms[room_id]
        if sid in room['users']:
            room['users'].remove(sid)
            
        if room['host'] == sid:
            await sio.emit('broadcast', {'type': 'kicked', 'reason': 'The host has left the room.'}, room=room_id)
            del rooms[room_id]
        else:
            await sio.emit('user-count', {'count': len(room['users'])}, room=room_id)
    
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
            
        await sio.emit('broadcast', data, room=room_id, skip_sid=sid)

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8000))
    web.run_app(app, host='0.0.0.0', port=port)
