import { useEffect, useRef, useState } from "react";

const TOAST_LIMIT = 5;
const TOAST_REMOVE_DELAY = 4000;

type ToastVariant = "default" | "destructive";

export interface Toast {
  id: string;
  title?: string;
  description?: string;
  variant?: ToastVariant;
  open: boolean;
}

type ToastInput = Omit<Toast, "id" | "open">;

type Listener = (toasts: Toast[]) => void;

let toasts: Toast[] = [];
const listeners: Set<Listener> = new Set();

function notify() {
  listeners.forEach((l) => l([...toasts]));
}

let count = 0;
function genId() {
  return String(++count);
}

function addToast(input: ToastInput) {
  const id = genId();
  toasts = [{ ...input, id, open: true }, ...toasts].slice(0, TOAST_LIMIT);
  notify();

  setTimeout(() => {
    toasts = toasts.map((t) => (t.id === id ? { ...t, open: false } : t));
    notify();
    setTimeout(() => {
      toasts = toasts.filter((t) => t.id !== id);
      notify();
    }, 300);
  }, TOAST_REMOVE_DELAY);
}

export function toast(input: ToastInput) {
  addToast(input);
}

export function useToast() {
  const [state, setState] = useState<Toast[]>([...toasts]);

  useEffect(() => {
    listeners.add(setState);
    return () => {
      listeners.delete(setState);
    };
  }, []);

  return {
    toasts: state,
    toast: (input: ToastInput) => addToast(input),
    dismiss: (id: string) => {
      toasts = toasts.map((t) => (t.id === id ? { ...t, open: false } : t));
      notify();
    },
  };
}
