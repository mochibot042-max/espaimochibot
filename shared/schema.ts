import { z } from "zod";

// Type definitions for interactions
export type Interaction = {
  id: number;
  transcript: string;
  response: string;
  volume: string;
  createdAt: Date;
};

export type InsertInteraction = Omit<Interaction, "id" | "createdAt">;

export const insertInteractionSchema = z.object({
  transcript: z.string(),
  response: z.string(),
  volume: z.string().default("1.0"),
});

// Type definitions for settings
export type Setting = {
  id: number;
  key: string;
  value: string;
};

export type InsertSetting = Omit<Setting, "id">;

export const insertSettingSchema = z.object({
  key: z.string(),
  value: z.string(),
});
