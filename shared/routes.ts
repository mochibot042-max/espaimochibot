import { z } from 'zod';
import { interactions } from './schema';

export const errorSchemas = {
  internal: z.object({ message: z.string() })
};

export const api = {
  interactions: {
    list: {
      method: 'GET' as const,
      path: '/api/interactions' as const,
      responses: {
        200: z.array(z.custom<typeof interactions.$inferSelect>()),
      },
    },
  },
};

export function buildUrl(path: string, params?: Record<string, string | number>): string {
  let url = path;
  if (params) {
    Object.entries(params).forEach(([key, value]) => {
      if (url.includes(`:${key}`)) {
        url = url.replace(`:${key}`, String(value));
      }
    });
  }
  return url;
}
