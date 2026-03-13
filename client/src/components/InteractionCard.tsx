import { motion } from "framer-motion";
import { format } from "date-fns";
import { Mic, Cpu, Clock } from "lucide-react";
import type { Interaction } from "@/hooks/use-interactions";

interface InteractionCardProps {
  interaction: Interaction;
  index: number;
}

export function InteractionCard({ interaction, index }: InteractionCardProps) {
  const formattedTime = interaction.createdAt
    ? format(interaction.createdAt, "MMM d, h:mm:ss a")
    : "Unknown time";

  return (
    <motion.div
      initial={{ opacity: 0, y: 15 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.4, delay: Math.min(index * 0.05, 0.5), ease: "easeOut" }}
      className="group relative flex flex-col gap-4 rounded-2xl bg-card p-5 sm:p-6 shadow-sm border border-border/50 hover:shadow-md hover:border-border transition-all duration-300"
    >
      {/* Timestamp */}
      <div className="absolute top-5 right-5 sm:top-6 sm:right-6 flex items-center gap-1.5 text-xs text-muted-foreground">
        <Clock className="w-3.5 h-3.5" />
        {formattedTime}
      </div>

      {/* Transcript (User Input) */}
      <div className="flex gap-4 pr-24">
        <div className="flex-shrink-0 mt-1">
          <div className="flex items-center justify-center w-8 h-8 rounded-full bg-zinc-100 text-zinc-600 dark:bg-zinc-900 dark:text-zinc-400 group-hover:bg-primary group-hover:text-primary-foreground transition-colors duration-300">
            <Mic className="w-4 h-4" />
          </div>
        </div>
        <div className="flex flex-col gap-1">
          <span className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">
            User Said
          </span>
          <p className="text-sm sm:text-base text-foreground font-medium leading-relaxed">
            "{interaction.transcript}"
          </p>
        </div>
      </div>

      {/* Spacer line */}
      <div className="pl-12">
        <div className="h-px w-full bg-border/40 my-1"></div>
      </div>

      {/* Response (ESP32 / AI Output) */}
      <div className="flex gap-4">
        <div className="flex-shrink-0 mt-1">
          <div className="flex items-center justify-center w-8 h-8 rounded-full bg-blue-50 text-blue-600 dark:bg-blue-950/30 dark:text-blue-400">
            <Cpu className="w-4 h-4" />
          </div>
        </div>
        <div className="flex flex-col gap-1">
          <span className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">
            Assistant Replied
          </span>
          <p className="text-sm sm:text-base text-muted-foreground leading-relaxed">
            {interaction.response}
          </p>
        </div>
      </div>
    </motion.div>
  );
}
