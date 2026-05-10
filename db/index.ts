import { neon } from "@neondatabase/serverless";
import { drizzle } from "drizzle-orm/neon-http";
import * as schema from "./schema";

const sql = neon("postgresql://neondb_owner:npg_q7LFnApNwM0o@ep-withered-darkness-aqli9vyo.c-8.us-east-1.aws.neon.tech/neondb?sslmode=require");
export const db = drizzle(sql, { schema });
