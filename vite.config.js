import { defineConfig } from "vite";

export default defineConfig({
  root: "ui",
  publicDir: "public",
  build: {
    outDir: "../dist",
    emptyOutDir: true,
  },
  server: {
    port: 5174,
    proxy: {
      "/scout-data": "http://localhost:8760",
      "/run-scout": "http://localhost:8760",
      "/audit": "http://localhost:8760",
      "/case": "http://localhost:8760",
      "/scout": "http://localhost:8760",
    },
  },
});
