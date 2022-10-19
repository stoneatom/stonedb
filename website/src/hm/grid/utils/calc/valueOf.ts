
export function valueOf(str: string) {
  const value = String(str).replace(/[^0-9\+\*\-\/\(\).]/g, '');
  return value ? Number(value) : 0;
}