import { useQuery } from "@tanstack/react-query";
import { api } from "@shared/routes";
import { z } from "zod";

// Zod coercions to handle JSON date strings properly
const interactionResponseSchema = z.object({
  id: z.number(),
  transcript: z.string(),
  response: z.string(),
  volume: z.string().optional().default("1.0"),
  createdAt: z.coerce.date().nullable(),
});

export type Interaction = z.infer<typeof interactionResponseSchema>;

export function useInteractions() {
  return useQuery({
    queryKey: ["/api/interactions"],
    queryFn: async () => {
      const res = await fetch("/api/interactions", { credentials: "include" });
      if (!res.ok) {
        throw new Error("Failed to fetch interactions");
      }
      const data = await res.json();
      
      // Parse with logging to handle any silent schema mismatches
      const result = z.array(interactionResponseSchema).safeParse(data);
      if (!result.success) {
        console.error("[Zod] interactions.list validation failed:", result.error.format());
        throw result.error;
      }
      return result.data;
    },
    // Auto-refresh every 5 seconds to provide a "live" feel 
    // while the user is talking to the ESP32
    refetchInterval: 5000, 
  });
}
