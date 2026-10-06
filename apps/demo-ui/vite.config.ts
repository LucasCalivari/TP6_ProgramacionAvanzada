import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import path from 'path';

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      '@activation-poc/contracts': path.resolve(__dirname, '../../libs/contracts/src'),
    },
  },
  server: {
    port: 5173,
    host: true,
  },
});
