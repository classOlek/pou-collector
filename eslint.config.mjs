import js from '@eslint/js';
import tseslint from 'typescript-eslint';

// Collector + shared are Node/TypeScript only — no browser, no JSX. The
// typescript-eslint recommended config disables core no-undef (TS resolves
// globals), so no explicit environment globals are needed here.
export default tseslint.config(
  {
    ignores: ['**/dist/**', '**/node_modules/**', '**/coverage/**'],
  },
  js.configs.recommended,
  ...tseslint.configs.recommended,
);
