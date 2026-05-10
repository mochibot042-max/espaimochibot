import { db } from "./db";
import { conversations, users, userPreferences } from "./db/schema";
import { eq, and, asc, desc, sql } from "drizzle-orm";

export interface Interaction {
  transcript: string;
  response: string;
  userId?: number;
}

export interface Message {
  role: "user" | "assistant";
  content: string;
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
    await db.delete(conversations).where(eq(conversations.userId, user.id));
    await db.delete(userPreferences).where(eq(userPreferences.userId, user.id));
    await db.delete(users).where(eq(users.id, user.id));
    return true;
  },

  // ========== CONVERSATION MEMORY (MAX 10, FIFO) ==========
  
  async addMessage(userId: number, role: "user" | "assistant", content: string) {
    // Insert new message
    await db.insert(conversations).values({
      userId,
      role,
      content,
    });

    // Keep only latest 10 messages (FIFO - delete oldest if > 10)
    const allMessages = await db
      .select()
      .from(conversations)
      .where(eq(conversations.userId, userId))
      .orderBy(asc(conversations.timestamp));

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
      .orderBy(asc(conversations.timestamp))
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
    // Delete existing name preference for this user
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

  // ========== LEGACY (for compatibility) ==========
  
  async createInteraction(data: Interaction) {
    if (data.userId) {
      await this.addMessage(data.userId, "user", data.transcript);
      await this.addMessage(data.userId, "assistant", data.response);
    }
  },
};
