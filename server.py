#!/usr/bin/env python3
import asyncio, json, websockets, os, uuid
from datetime import datetime

CLIENTS = {}
MAX_CLIENTS = 5

async def broadcast(msg, exclude=None):
    for ws in list(CLIENTS):
        if ws != exclude:
            try: await ws.send(json.dumps(msg))
            except: pass

async def send_user_list():
    users = [{"id": v["id"], "name": v["name"]} for v in CLIENTS.values()]
    await broadcast({"type": "user_list", "users": users})

async def handler(websocket):
    cid = str(uuid.uuid4())[:8]
    if len(CLIENTS) >= MAX_CLIENTS:
        await websocket.send(json.dumps({"type":"error","message":"Chat lleno (máx 5)"}))
        return
    CLIENTS[websocket] = {"id": cid, "name": f"Usuario_{cid[:4]}"}
    await websocket.send(json.dumps({"type":"welcome","id":cid,"name":CLIENTS[websocket]["name"]}))
    await send_user_list()
    await broadcast({"type":"system","message":f"{CLIENTS[websocket]['name']} se unió 🟢"}, exclude=websocket)
    try:
        async for raw in websocket:
            data = json.loads(raw)
            t = data.get("type")
            if t == "rename":
                old = CLIENTS[websocket]["name"]
                new = data.get("name", old)[:24]
                CLIENTS[websocket]["name"] = new
                await send_user_list()
                await broadcast({"type":"system","message":f"{old} ahora es {new} ✏️"})
            elif t == "request":
                for ws2, info in CLIENTS.items():
                    if info["id"] == data.get("to"):
                        await ws2.send(json.dumps({"type":"request","from_id":cid,"from_name":CLIENTS[websocket]["name"]}))
                        break
            elif t == "request_response":
                for ws2, info in CLIENTS.items():
                    if info["id"] == data.get("to"):
                        await ws2.send(json.dumps({"type":"request_response","from_id":cid,"from_name":CLIENTS[websocket]["name"],"accepted":data.get("accepted",False)}))
                        break
            elif t == "group_invite":
                for ws2, info in CLIENTS.items():
                    if info["id"] == data.get("to"):
                        await ws2.send(json.dumps({"type":"group_invite","from_id":cid,"from_name":CLIENTS[websocket]["name"],"room":data.get("room"),"members":data.get("members")}))
                        break
            elif t == "message":
                msg = {"type":"message","from_id":cid,"from_name":CLIENTS[websocket]["name"],
                       "content":data.get("content",""),"msg_type":data.get("msg_type","text"),
                       "file_name":data.get("file_name",""),"file_data":data.get("file_data",""),
                       "room":data.get("room",""),"timestamp":datetime.now().strftime("%H:%M")}
                members = data.get("room","").split(",")
                for ws2, info in CLIENTS.items():
                    if info["id"] in members:
                        try: await ws2.send(json.dumps(msg))
                        except: pass
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        name = CLIENTS.pop(websocket, {}).get("name","alguien")
        await send_user_list()
        await broadcast({"type":"system","message":f"{name} salió 🔴"})

async def main():
    port = int(os.environ.get("PORT", 8766))
    print(f"WebSocket server corriendo en puerto {port}")
    async with websockets.serve(handler, "0.0.0.0", port, max_size=60*1024*1024):
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())