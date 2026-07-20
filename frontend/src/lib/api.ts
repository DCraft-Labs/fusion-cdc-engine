import axios from "axios";

export const api = axios.create({
  baseURL: "/api/v1",
  headers: { "Content-Type": "application/json" },
});

api.interceptors.request.use((config) => {
  const token = localStorage.getItem("auth_token");
  if (token) {
    config.headers.Authorization = `Bearer ${token}`;
  }
  return config;
});

api.interceptors.response.use(
  (response) => response,
  (error) => {
    if (error.response?.status === 401 && !error.config?.url?.includes("/auth/login")) {
      localStorage.removeItem("auth_token");
      window.location.href = "/login";
    }
    return Promise.reject(error);
  }
);

/**
 * Extract array from a paginated API response.
 * Backend returns: { sources: [...], total, page, page_size }
 * This helper finds the array property in the response object.
 * If the response is already an array, returns it as-is.
 */
export function extractList<T = any>(data: any, key?: string): T[] {
  if (Array.isArray(data)) return data;
  if (!data || typeof data !== "object") return [];
  if (key && Array.isArray(data[key])) return data[key];
  // Auto-detect: find the first array property
  for (const k of Object.keys(data)) {
    if (Array.isArray(data[k])) return data[k];
  }
  return [];
}

/**
 * Fetch a list endpoint and automatically extract the array.
 * Usage: fetchList("/sources") or fetchList("/sources", "sources")
 */
export async function fetchList<T = any>(url: string, key?: string, params?: Record<string, any>): Promise<T[]> {
  const { data } = await api.get(url, { params });
  return extractList<T>(data, key);
}

/**
 * Fetch a single resource. Returns null on 404/error.
 */
export async function fetchOne<T = any>(url: string): Promise<T | null> {
  try {
    const { data } = await api.get(url);
    return data;
  } catch {
    return null;
  }
}
