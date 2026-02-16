/**
 * Vercel serverless entrypoint.
 * Exports the NestJS app as a request handler for Vercel Functions.
 */
import type { VercelRequest, VercelResponse } from '@vercel/node';
import { createNestApp } from './app-factory';

let cachedHandler: ((req: import('express').Request, res: import('express').Response) => void) | null =
  null;

async function getHandler() {
  if (cachedHandler) return cachedHandler;

  const app = await createNestApp();
  await app.init();

  const expressApp = app.getHttpAdapter().getInstance();
  cachedHandler = expressApp;

  return expressApp;
}

export default async function handler(req: VercelRequest, res: VercelResponse) {
  const expressApp = await getHandler();
  expressApp(req, res);
}
