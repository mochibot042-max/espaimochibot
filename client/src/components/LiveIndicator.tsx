import { motion } from "framer-motion";

export function LiveIndicator() {
  return (
    <div className="flex items-center gap-2 px-3 py-1.5 rounded-full bg-emerald-50 border border-emerald-100 dark:bg-emerald-950/30 dark:border-emerald-900/50">
      <div className="relative flex h-2 w-2">
        <motion.span
          animate={{ scale: [1, 1.5, 1], opacity: [0.7, 0, 0.7] }}
          transition={{ duration: 2, repeat: Infinity, ease: "easeInOut" }}
          className="absolute inline-flex h-full w-full rounded-full bg-emerald-500 opacity-75"
        />
        <span className="relative inline-flex rounded-full h-2 w-2 bg-emerald-500"></span>
      </div>
      <span className="text-xs font-medium text-emerald-700 dark:text-emerald-400">
        Live Connection
      </span>
    </div>
  );
}
