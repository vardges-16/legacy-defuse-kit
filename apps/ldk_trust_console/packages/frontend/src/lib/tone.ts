//-----------------------------------------------------------------------
// Static Tailwind class maps per status tone (static so Tailwind can see them).
//-----------------------------------------------------------------------

import type { Tone } from './format';

export const toneText: Record<Tone, string> = {
  ok: 'text-ok',
  warn: 'text-warn',
  bad: 'text-bad',
  idle: 'text-muted-foreground',
};

export const toneSoft: Record<Tone, string> = {
  ok: 'bg-ok-soft text-ok',
  warn: 'bg-warn-soft text-warn',
  bad: 'bg-bad-soft text-bad',
  idle: 'bg-muted text-muted-foreground',
};

export const toneDot: Record<Tone, string> = {
  ok: 'bg-ok',
  warn: 'bg-warn',
  bad: 'bg-bad',
  idle: 'bg-muted-foreground',
};
