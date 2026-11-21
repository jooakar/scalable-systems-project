import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// The bundle is written straight into the web module's classpath so the Java
// server (and the shaded jar) serves it from /static.
export default defineConfig({
  plugins: [react()],
  build: {
    outDir: "../target/classes/static",
    emptyOutDir: true,
  },
  server: {
    port: 5173,
    proxy: {
      "/api": process.env.API_TARGET ?? "http://localhost:7070",
      "/health": process.env.API_TARGET ?? "http://localhost:7070",
    },
  },
});
