import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import { resolve } from "path";

export default defineConfig(({ mode }) => ({
  plugins: [react(), tailwindcss()],
  test: {
    include: ["src/**/*.test.{ts,tsx}"],
    exclude: ["e2e/**"],
  },
  resolve: {
    alias: {
      "@": resolve(__dirname, "./src"),
    },
  },
  build: {
    outDir: "../static",
    emptyOutDir: true,
  },
  server: {
    proxy: mode === "operations" ? undefined : {
      "/api": "http://localhost:3000",
    },
  },
}));
