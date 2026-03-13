import { type InsertInteraction, type Interaction, type Setting } from "@shared/schema";

export interface IStorage {
  getInteractions(): Promise<Interaction[]>;
  createInteraction(interaction: InsertInteraction): Promise<Interaction>;
  getSetting(key: string): Promise<Setting | undefined>;
  setSetting(key: string, value: string): Promise<void>;
}

export class InMemoryStorage implements IStorage {
  private interactions: Interaction[] = [];
  private settings: Map<string, string> = new Map();
  private nextId = 1;

  async getInteractions(): Promise<Interaction[]> {
    return [...this.interactions].reverse().slice(0, 50);
  }

  async createInteraction(interaction: InsertInteraction): Promise<Interaction> {
    const created: Interaction = {
      id: this.nextId++,
      ...interaction,
      createdAt: new Date(),
    };
    this.interactions.push(created);
    return created;
  }

  async getSetting(key: string): Promise<Setting | undefined> {
    const value = this.settings.get(key);
    if (value === undefined) return undefined;
    return { id: 0, key, value };
  }

  async setSetting(key: string, value: string): Promise<void> {
    this.settings.set(key, value);
  }
}

export const storage = new InMemoryStorage();
