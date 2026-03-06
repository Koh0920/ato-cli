import { Box, ExternalLink, Globe, Package, Play, Search, Square, Trash2, Zap } from "lucide-react";
import type { Capsule, Process } from "../types";

interface CapsuleRowProps {
  capsule: Capsule;
  activeProcess: Process | undefined;
  openReady: boolean;
  platform: string;
  onRun: (capsule: Capsule) => void;
  onStop: (capsule: Capsule) => void;
  onOpen: (capsule: Capsule, process?: Process) => void;
  onInspect: (capsule: Capsule) => void;
  onDelete: (capsule: Capsule) => void;
}

function IconForCapsule({ iconKey }: { iconKey: Capsule["iconKey"] }): JSX.Element {
  const common = { size: 16, strokeWidth: 1.5 };
  if (iconKey === "globe") {
    return <Globe {...common} />;
  }
  if (iconKey === "zap") {
    return <Zap {...common} />;
  }
  if (iconKey === "box") {
    return <Box {...common} />;
  }
  return <Package {...common} />;
}

export function CapsuleRow({
  capsule,
  activeProcess,
  openReady,
  platform,
  onRun,
  onStop,
  onOpen,
  onInspect,
  onDelete,
}: CapsuleRowProps): JSX.Element {
  const isRunning = Boolean(activeProcess?.active);

  return (
    <tr
      className="table-row"
      role="button"
      tabIndex={0}
      onClick={() => onInspect(capsule)}
      onKeyDown={(event) => {
        if (event.key === "Enter") {
          onInspect(capsule);
        }
      }}
    >
      <td className="table-status">
        <span className={`table-dot ${isRunning ? "active" : ""}`} aria-label={isRunning ? "Running" : "Stopped"} />
      </td>
      <td>
        <div className="capsule-cell">
          <div className="capsule-cell-icon">
            {capsule.storeMetadata?.iconUrl ? (
              <img
                src={capsule.storeMetadata.iconUrl}
                alt={`${capsule.scopedId} icon`}
                className="capsule-cell-icon-img"
              />
            ) : (
              <IconForCapsule iconKey={capsule.iconKey} />
            )}
          </div>
          <div>
            <div className="row-id">{capsule.scopedId}</div>
            <div className="row-desc">{capsule.description}</div>
          </div>
        </div>
      </td>
      <td className="row-meta">{capsule.version}</td>
      <td>
        <div className="compat-list">
          {capsule.osArch.map((entry) => (
            <span key={entry} className={`badge ${entry === platform ? "badge-accent" : "badge-muted"}`}>
              {entry}
            </span>
          ))}
        </div>
      </td>
      <td className="row-meta">{capsule.size}</td>
      <td className="table-actions-cell">
        <div className="actions-list" onClick={(event) => event.stopPropagation()}>
          {isRunning ? (
            <button className="btn btn-danger" type="button" onClick={() => onStop(capsule)}>
              <Square size={14} strokeWidth={1.5} /> Stop
            </button>
          ) : (
            <button className="btn btn-success" type="button" onClick={() => onRun(capsule)}>
              <Play size={14} strokeWidth={1.5} /> Run
            </button>
          )}
          {isRunning ? (
            <button
              className="btn btn-ghost"
              type="button"
              onClick={() => onOpen(capsule, activeProcess)}
              disabled={!openReady}
            >
              <ExternalLink size={14} strokeWidth={1.5} /> Open
            </button>
          ) : null}
          <button className="btn btn-ghost" type="button" onClick={() => onInspect(capsule)}>
            <Search size={14} strokeWidth={1.5} /> Inspect
          </button>
          <button className="btn btn-danger" type="button" onClick={() => onDelete(capsule)}>
            <Trash2 size={14} strokeWidth={1.5} /> Delete
          </button>
        </div>
      </td>
    </tr>
  );
}
