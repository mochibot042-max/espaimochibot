import { motion } from "framer-motion";
import { Mic } from "lucide-react";

export function EmptyState() {
  return (
    <motion.div
      initial={{ opacity: 0, scale: 0.95 }}
      animate={{ opacity: 1, scale: 1 }}
      transition={{ duration: 0.5 }}
      className="flex flex-col items-center justify-center py-24 px-4 text-center"
    >
      <div className="relative mb-6">
        <div className="absolute inset-0 rounded-full bg-primary/5 animate-ping" style={{ animationDuration: '3s' }} />
        <div className="relative flex items-center justify-center w-20 h-20 rounded-full bg-primary/5 text-primary">
          <Mic className="w-8 h-8" />
        </div>
      </div>
      <h3 className="text-xl font-semibold mb-2">Awaiting your voice</h3>
      <p className="text-muted-foreground max-w-sm">
        Press the button on your ESP32 and start speaking. Your interactions will appear here instantly.
      </p>
    </motion.div>
  );
}
