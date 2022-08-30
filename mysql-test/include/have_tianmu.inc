if (`SELECT COUNT(*) = 0 FROM INFORMATION_SCHEMA.ENGINES WHERE engine = 'tianmu' AND support IN ('YES', 'DEFAULT', 'ENABLED')`)
{
  --skip Test requires Tianmu.
}
