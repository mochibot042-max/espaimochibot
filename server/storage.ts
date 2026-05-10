import { db } from "./db/index.js";
import { conversations, users, userPreferences } from "./db/schema.js";
import { eq, and, asc } from "drizzle-orm";
import { sql } from "drizzle-orm";

export interface Message {
  role: "user" | "assistant";
  content: string;
}

// ========== AUTO PUSH SCHEMA ON STARTUP ==========
export async function pushSchema() {
  console.log("[DB] Starting schema push...");
  
  try {
    // Create users table first (no dependencies)
    await db.execute(sql`
      CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL UNIQUE,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
      );
    `);
    console.log("[DB] users table OK");
    
    // Create conversations table (depends on users)
    await db.execute(sql`
      CREATE TABLE IF NOT EXISTS conversations (
        id SERIAL PRIMARY KEY,
        user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
        role TEXT NOT NULL,
        content TEXT NOT NULL,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
      );
    `);
    console.log("[DB] conversations table OK");
    
    // Create user_preferences table (depends on users)
    await db.execute(sql`
      CREATE TABLE IF NOT EXISTS user_preferences (
        id SERIAL PRIMARY KEY,
        user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
        key TEXT NOT NULL,
        value TEXT NOT NULL,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
      );
    `);
    console.log("[DB] user_preferences table OK");
    
    // Create indexes
    await db.execute(sql`CREATE INDEX IF NOT EXISTS idx_conversations_user_id ON conversations(user_id);`);
    await db.execute(sql`CREATE INDEX IF NOT EXISTS idx_conversations_created_at ON conversations(created_at);`);
    await db.execute(sql`CREATE INDEX IF NOT EXISTS idx_preferences_user_key ON user_preferences(user_id, key);`);
    console.log("[DB] indexes OK");
    
    console.log("[DB] Schema push completed successfully");
  } catch (e: any) {
    console.error("[DB] Schema push failed:", e.message);
    throw e;
  }
}

// ========== VERIFY SCHEMA EXISTS ==========
export async function verifySchema(): Promise<boolean> {
  try {
    await db.select().from(users).limit(1);
    await db.select().from(conversations).limit(1);
    await db.select().from(userPreferences).limit(1);
    return true;
  } catch {
    return false;
  }
}

export const storage = {
  // ========== USER MANAGEMENT ==========
  
  async createUser(name: string) {
    const [user] = await db.insert(users).values({ name }).returning();
    return user;
  },

  async getUserByName(name: string) {
    const [user] = await db.select().from(users).where(eq(users.name, name));
    return user;
  },

  async getOrCreateUser(name: string) {
    let user = await this.getUserByName(name);
    if (!user) {
      user = await this.createUser(name);
    }
    return user;
  },

  async deleteUser(name: string) {
    const user = await this.getUserByName(name);
    if (!user) return false;
    await db.delete(users).where(eq(users.id, user.id));
    return true;
  },

  // ========== CONVERSATION MEMORY (MAX 10, FIFO) ==========
  
  async addMessage(userId: number, role: "user" | "assistant", content: string) {
    await db.insert(conversations).values({
      userId,
      role,
      content,
    });

    // FIFO: Keep only latest 10 messages
    const allMessages = await db
      .select()
      .from(conversations)
      .where(eq(conversations.userId, userId))
      .orderBy(asc(conversations.createdAt));

    if (allMessages.length > 10) {
      const toDelete = allMessages.length - 10;
      const oldestIds = allMessages.slice(0, toDelete).map(m => m.id);
      
      for (const id of oldestIds) {
        await db.delete(conversations).where(eq(conversations.id, id));
      }
    }
  },

  async getConversationHistory(userId: number): Promise<Message[]> {
    const messages = await db
      .select()
      .from(conversations)
      .where(eq(conversations.userId, userId))
      .orderBy(asc(conversations.createdAt))
      .limit(10);

    return messages.map(m => ({
      role: m.role as "user" | "assistant",
      content: m.content,
    }));
  },

  async clearConversation(userId: number) {
    await db.delete(conversations).where(eq(conversations.userId, userId));
  },

  // ========== NAME MEMORY (PERSISTENT) ==========
  
  async saveName(userId: number, name: string) {
    await db
      .delete(userPreferences)
      .where(
        and(
          eq(userPreferences.userId, userId),
          eq(userPreferences.key, "name")
        )
      );

    await db.insert(userPreferences).values({
      userId,
      key: "name",
      value: name,
    });
  },

  async getSavedName(userId: number): Promise<string | null> {
    const [pref] = await db
      .select()
      .from(userPreferences)
      .where(
        and(
          eq(userPreferences.userId, userId),
          eq(userPreferences.key, "name")
        )
      );
    return pref?.value || null;
  },

  async deleteSavedName(userId: number) {
    await db
      .delete(userPreferences)
      .where(
        and(
          eq(userPreferences.userId, userId),
          eq(userPreferences.key, "name")
        )
      );
  },

  // ========== LEGACY ==========
  
  async createInteraction(data: { transcript: string; response: string; userId?: number }) {
    if (data.userId) {
      await this.addMessage(data.userId, "user", data.transcript);
      await this.addMessage(data.userId, "assistant", data.response);
    }
  },
};
