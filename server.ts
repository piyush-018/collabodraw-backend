import express from 'express';
import { createServer } from 'http';
import { Server, Socket } from 'socket.io';
import cors from 'cors';
import fs from 'fs';
import path from 'path';

const app = express();
app.use(cors());

const httpServer = createServer(app);

const io = new Server(httpServer, {
  cors: {
    origin: "*", 
    methods: ["GET", "POST"]
  }
});

// 🔴 1. LOCAL FILE DATABASE SETUP (Zero Dependencies)
const DB_FILE = path.resolve(__dirname, 'boards_db.json');

// Load initial state from the JSON file on server startup
let boardsState: Record<string, any[]> = {};
if (fs.existsSync(DB_FILE)) {
  try {
    const data = fs.readFileSync(DB_FILE, 'utf-8');
    boardsState = JSON.parse(data);
    console.log('📦 Loaded existing boards from local JSON database.');
  } catch (err) {
    console.error('Error reading local DB file:', err);
  }
} else {
  console.log('🆕 No existing DB found. Starting fresh.');
}

const pendingSaves: Set<string> = new Set(); // Tracks modified boards

// 🔴 2. REAL-TIME HYBRID CACHE ARCHITECTURE
io.on('connection', (socket: Socket) => {
  console.log(`🟢 User connected: ${socket.id}`);

  socket.on('join-board', (boardId: string) => {
    socket.join(boardId);
    console.log(`User ${socket.id} joined board: ${boardId}`);
    
    // Send data from fast RAM cache instantly
    if (boardsState[boardId]) {
      socket.emit('board-state-sync', boardsState[boardId]);
    }

    const clientsInRoom = io.sockets.adapter.rooms.get(boardId)?.size || 0;
    io.to(boardId).emit('users-count', clientsInRoom);
  });

  socket.on('draw', (data: { boardId: string, shapes: any[] }) => {
    // Update RAM cache
    boardsState[data.boardId] = data.shapes;
    // Mark for background file sync
    pendingSaves.add(data.boardId);

    socket.to(data.boardId).emit('draw', data.shapes);
  });

  socket.on('cursor-move', (data: any) => {
    socket.to(data.boardId).emit('cursor-move', { ...data, socketId: socket.id });
  });

  socket.on('draft-update', (data: any) => {
    socket.to(data.boardId).emit('draft-update', { shape: data.shape, socketId: socket.id });
  });

  socket.on('draft-end', (data: any) => {
    socket.to(data.boardId).emit('draft-end', { socketId: socket.id });
  });

  socket.on('disconnecting', () => {
    for (const room of socket.rooms) {
      if (room !== socket.id) {
        socket.to(room).emit('user-disconnected', socket.id);
        const clientsInRoom = (io.sockets.adapter.rooms.get(room)?.size || 1) - 1;
        io.to(room).emit('users-count', clientsInRoom);
      }
    }
  });

  socket.on('disconnect', () => {
    console.log(`🔴 User disconnected: ${socket.id}`);
  });
});

// 🔴 3. BACKGROUND WORKER (Auto-Save to JSON file every 5 seconds)
setInterval(() => {
  if (pendingSaves.size === 0) return;

  try {
    // Write the entire boardsState object to the file
    fs.writeFileSync(DB_FILE, JSON.stringify(boardsState, null, 2));
    // console.log(`💾 Auto-saved ${pendingSaves.size} active boards to disk.`);
    pendingSaves.clear();
  } catch (err) {
    console.error('Failed to auto-save to JSON DB:', err);
  }
}, 5000);

// 🔴 4. GRACEFUL SHUTDOWN (Claude's pro-tip to prevent data loss)
const handleShutdown = () => {
  if (pendingSaves.size > 0) {
    console.log('\n🛑 Shutting down! Flushing final drawing data to disk...');
    try {
      fs.writeFileSync(DB_FILE, JSON.stringify(boardsState, null, 2));
      console.log('✅ Final data saved successfully.');
    } catch (err) {
      console.error('❌ Failed to save final data:', err);
    }
  } else {
    console.log('\n🛑 Shutting down gracefully. No pending saves.');
  }
  process.exit(0);
};

process.on('SIGINT', handleShutdown);  // Catches Ctrl+C
process.on('SIGTERM', handleShutdown); // Catches server kill commands

const PORT = process.env.PORT || 4000;
httpServer.listen(PORT, () => {
  console.log(`🚀 Real-Time Server running on port ${PORT}`);
});