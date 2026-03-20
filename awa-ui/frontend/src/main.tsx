import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import "./app.css";
import { App } from "./App";
import { initializeTheme } from "@/hooks/use-theme";

// Set up system theme listener (handles OS dark mode changes)
initializeTheme();

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <App />
  </StrictMode>
);
