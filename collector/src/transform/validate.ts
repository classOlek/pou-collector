/**
 * Validation gate (docs/ARCHITECTURE.md §7: validate *before* deleting raw).
 *
 * Runs against the transform's in-memory output before anything is published or
 * any raw shard is deleted. If it fails, the caller keeps raw and does not
 * publish — a transform bug can never destroy the only copy of the data or ship
 * a degenerate snapshot. Coverage honesty (Phase 4 #12) is enforced here: the
 * published meta must agree with the manifest's outcome counts.
 */
import type { AggregateFile, Coverage, SnapshotMeta } from '@pou/shared';
import { SCHEMA_VERSION } from '@pou/shared';

export interface ValidationInput {
  meta: SnapshotMeta;
  coverage: Coverage;
  /** Characters still awaiting computation (0 on a final publish). */
  pendingCount: number;
  /** Characters marked skipped when the snapshot was closed. */
  skippedCount: number;
  /** Characters seeded into the snapshot from the roster. */
  totalCharacters: number;
  characterRowCount: number;
  aggregates: AggregateFile[];
  /** Sum of class_distribution counts — every character has exactly one class. */
  classDistributionTotal: number;
}

export interface ValidationResult {
  ok: boolean;
  errors: string[];
}

export function validateTransform(input: ValidationInput): ValidationResult {
  const errors: string[] = [];
  const {
    meta,
    coverage,
    pendingCount,
    skippedCount,
    totalCharacters,
    characterRowCount,
    aggregates,
  } = input;

  if (meta.schemaVersion !== SCHEMA_VERSION) {
    errors.push(`meta schemaVersion ${meta.schemaVersion} != ${SCHEMA_VERSION}`);
  }
  if (!meta.ladderCapturedAt) errors.push('meta.ladderCapturedAt is missing');
  // A complete snapshot must carry its completion time and owe no characters;
  // an incomplete one must NOT claim completion.
  if (meta.complete) {
    if (!meta.completedAt) errors.push('meta.completedAt is missing on a complete snapshot');
    if (pendingCount !== 0) errors.push(`complete snapshot has ${pendingCount} pending characters`);
  } else {
    if (meta.completedAt) errors.push('incomplete snapshot must not set completedAt');
    if (pendingCount === 0) errors.push('incomplete snapshot has no pending characters');
  }
  if (meta.pendingCount !== pendingCount) {
    errors.push(`meta.pendingCount ${meta.pendingCount} != ${pendingCount}`);
  }
  if (meta.skippedCount !== skippedCount) {
    errors.push(`meta.skippedCount ${meta.skippedCount} != ${skippedCount}`);
  }

  // Coverage honesty: resolved + still-pending + skipped must add up to the
  // seeded queue (skipped = deliberately uncollected at close, never fetched).
  const summed = coverage.ok + coverage.private + coverage.dead;
  if (summed + pendingCount + skippedCount !== totalCharacters) {
    errors.push(
      `coverage sum ${summed} + pending ${pendingCount} + skipped ${skippedCount} ` +
        `!= total characters ${totalCharacters}`,
    );
  }

  // Characters row count must match the ok coverage (dedup already applied).
  if (characterRowCount !== coverage.ok) {
    errors.push(`characters row count ${characterRowCount} != coverage.ok ${coverage.ok}`);
  }
  if (meta.characterCount !== characterRowCount) {
    errors.push(`meta.characterCount ${meta.characterCount} != rows ${characterRowCount}`);
  }

  // A non-empty snapshot must produce non-degenerate aggregates.
  if (coverage.ok > 0) {
    if (characterRowCount === 0) errors.push('coverage.ok > 0 but no character rows');
    const classDist = aggregates.find((a) => a.kind === 'class_distribution');
    if (!classDist || classDist.rows.length === 0) {
      errors.push('class_distribution aggregate is empty for a non-empty snapshot');
    }
    // Every character contributes exactly one class → the distribution covers all.
    if (input.classDistributionTotal !== characterRowCount) {
      errors.push(
        `class_distribution counts ${input.classDistributionTotal} != characters ${characterRowCount}`,
      );
    }
  } else {
    errors.push('snapshot has zero collected characters (nothing to publish)');
  }

  // Aggregate percentages must reference the character total consistently.
  for (const agg of aggregates) {
    if (agg.total !== characterRowCount) {
      errors.push(`${agg.kind} total ${agg.total} != characters ${characterRowCount}`);
    }
  }

  return { ok: errors.length === 0, errors };
}

export class TransformValidationError extends Error {
  constructor(public readonly errors: string[]) {
    super(`transform validation failed: ${errors.join('; ')}`);
    this.name = 'TransformValidationError';
  }
}
