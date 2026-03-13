import { useInteractions } from "@/hooks/use-interactions";
import { InteractionCard } from "@/components/InteractionCard";
import { EmptyState } from "@/components/EmptyState";
import { LiveIndicator } from "@/components/LiveIndicator";
import { Activity, Radio } from "lucide-react";

export default function Dashboard() {
  const { data: interactions, isLoading, isError } = useInteractions();

  return (
    <div className="min-h-screen bg-background pb-20">
      {/* Navbar */}
      <header className="sticky top-0 z-50 glass-panel border-b">
        <div className="max-w-5xl mx-auto px-4 sm:px-6 h-16 flex items-center justify-between">
          <div className="flex items-center gap-2">
            <div className="flex items-center justify-center w-8 h-8 rounded-lg bg-primary text-primary-foreground">
              <Radio className="w-4 h-4" />
            </div>
            <h1 className="text-lg font-bold">ESP32 Voice AI</h1>
          </div>
          <LiveIndicator />
        </div>
      </header>

      {/* Main Content */}
      <main className="max-w-5xl mx-auto px-4 sm:px-6 pt-10">
        <div className="mb-8 flex flex-col gap-2">
          <h2 className="text-3xl font-bold tracking-tight">Interaction Log</h2>
          <p className="text-muted-foreground">
            A real-time dashboard of conversations with your hardware assistant.
          </p>
        </div>

        {/* State Handling */}
        {isLoading ? (
          <div className="space-y-4">
            {[1, 2, 3].map((i) => (
              <div key={i} className="w-full h-40 rounded-2xl bg-muted/30 animate-pulse border border-border/50"></div>
            ))}
          </div>
        ) : isError ? (
          <div className="p-6 rounded-2xl bg-destructive/10 text-destructive border border-destructive/20 flex flex-col items-center justify-center text-center">
            <Activity className="w-8 h-8 mb-3 opacity-50" />
            <h3 className="font-semibold text-lg">Connection Lost</h3>
            <p className="text-sm mt-1 opacity-80">Failed to retrieve logs. Retrying in background...</p>
          </div>
        ) : interactions && interactions.length > 0 ? (
          <div className="space-y-4">
            {/* Sort newest first */}
            {[...interactions].reverse().map((interaction, index) => (
              <InteractionCard 
                key={interaction.id} 
                interaction={interaction} 
                index={index} 
              />
            ))}
          </div>
        ) : (
          <EmptyState />
        )}
      </main>
    </div>
  );
}
