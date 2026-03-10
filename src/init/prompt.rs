use anyhow::{Context, Result};
use std::fs;
use std::path::{Path, PathBuf};

use crate::init::detect::{self, DetectedProject, NodePackageManager, ProjectType};
use crate::init::recipe::{self, ProjectInfo};
use crate::init::PromptArgs;

pub fn execute(
    args: PromptArgs,
    _reporter: std::sync::Arc<crate::reporters::CliReporter>,
) -> Result<()> {
    let project_dir = args
        .path
        .unwrap_or_else(|| PathBuf::from("."))
        .canonicalize()
        .context("Failed to resolve project directory")?;

    let detected = detect::detect_project(&project_dir)?;
    let info = recipe::project_info_from_detection(&detected)?;
    let context = PromptContext::from_project(&project_dir, &detected, &info)?;
    let prompt = render_prompt(&context);

    println!("Analyzing project...");
    println!("Found: {}", context.summary_line());
    if let Some(frameworks) = context.framework_hints_line() {
        println!("Framework hints: {frameworks}");
    }
    if let Some(ambiguity) = context.ambiguities.first() {
        println!("Ambiguity detected: {ambiguity}");
    }
    println!();
    println!("✨ Generated an agent-ready prompt for capsule.toml creation.");
    println!(
        "Copy the prompt below into your preferred AI agent, then validate the result with `ato validate capsule.toml`."
    );
    println!();
    println!("==================================================");
    println!("{prompt}");
    println!("==================================================");

    Ok(())
}

#[derive(Debug)]
struct PromptContext {
    project_dir: PathBuf,
    detected: DetectedProject,
    info: ProjectInfo,
    evidence_files: Vec<String>,
    important_dirs: Vec<String>,
    framework_hints: Vec<String>,
    ambiguities: Vec<String>,
    schema_constraints: Vec<String>,
    decision_rules: Vec<String>,
}

impl PromptContext {
    fn from_project(dir: &Path, detected: &DetectedProject, info: &ProjectInfo) -> Result<Self> {
        let evidence_files = collect_existing(
            dir,
            &[
                "package.json",
                "Cargo.toml",
                "requirements.txt",
                "pyproject.toml",
                "go.mod",
                "Gemfile",
                "next.config.js",
                "next.config.mjs",
                "next.config.ts",
            ],
        );
        let important_dirs = collect_existing(
            dir,
            &[
                "src", "app", "pages", "public", "dist", "build", "out", "static",
            ],
        );

        let framework_hints = detect_framework_hints(dir, detected)?;
        let ambiguities = detect_ambiguities(dir, detected, info, &framework_hints)?;

        let mut schema_constraints = vec![
            "Generate a valid `capsule.toml` for Ato `schema_version = \"0.2\"`.".to_string(),
            "Use `type = \"app\"` and include a valid `default_target` plus the matching `[targets.<name>]` table.".to_string(),
            "For source-executed apps, prefer `runtime = \"source\"` and set `entrypoint` to the executable with extra arguments in `cmd = [...]`.".to_string(),
            "Do not invent unsupported fields; if a required field is unclear, ask the user before generating TOML.".to_string(),
        ];
        let mut decision_rules = Vec::new();

        if framework_hints.iter().any(|hint| hint == "Next.js") {
            schema_constraints.push(
                "For static web export, use `[targets.static]`, `runtime = \"web\"`, `driver = \"static\"`, and point `entrypoint` at the exported directory (usually `out`).".to_string(),
            );
            decision_rules.push(
                "If the user confirms this is a static export, generate a `static` web target rooted at `out` unless the project facts show a different export directory.".to_string(),
            );
            decision_rules.push(
                "If the user confirms this is a dynamic server / SSR app, generate a `cli` source target using the release command hint (for example `npm start`).".to_string(),
            );
        } else if !info.entrypoint.is_empty() {
            decision_rules.push(format!(
                "Prefer the detected release command unless the user says it should be different: `{}`.",
                info.entrypoint.join(" ")
            ));
        }

        Ok(Self {
            project_dir: dir.to_path_buf(),
            detected: detected.clone(),
            info: ProjectInfo {
                name: info.name.clone(),
                project_type: info.project_type,
                entrypoint: info.entrypoint.clone(),
                node_dev_entrypoint: info.node_dev_entrypoint.clone(),
                node_release_entrypoint: info.node_release_entrypoint.clone(),
            },
            evidence_files,
            important_dirs,
            framework_hints,
            ambiguities,
            schema_constraints,
            decision_rules,
        })
    }

    fn summary_line(&self) -> String {
        let mut parts = vec![self.detected.project_type.as_str().to_string()];
        if !self.evidence_files.is_empty() {
            parts.push(format!("evidence: {}", self.evidence_files.join(", ")));
        }
        if let Some(node) = self.detected.node.as_ref() {
            parts.push(format!(
                "package manager: {}",
                node_package_manager_label(node.package_manager)
            ));
        }
        parts.join(" | ")
    }

    fn framework_hints_line(&self) -> Option<String> {
        if self.framework_hints.is_empty() {
            None
        } else {
            Some(self.framework_hints.join(", "))
        }
    }
}

fn render_prompt(context: &PromptContext) -> String {
    let mut lines = vec![
        "You are an expert Ato capsule configurator.".to_string(),
        String::new(),
        "Your task is to generate a valid `capsule.toml` for this project.".to_string(),
        "If any requirement is ambiguous, ask concise follow-up questions and wait for the user's answer before writing TOML.".to_string(),
        "Output only the final TOML inside a single ```toml fenced code block after all questions are answered.".to_string(),
        String::new(),
        "## Extracted project facts".to_string(),
        format!("- Project root: `{}`", context.project_dir.display()),
        format!("- Detected project type: {}", context.detected.project_type.as_str()),
        format!("- Suggested package name: `{}`", context.info.name),
    ];

    if !context.evidence_files.is_empty() {
        lines.push(format!(
            "- Evidence files: {}",
            context
                .evidence_files
                .iter()
                .map(|item| format!("`{item}`"))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if !context.important_dirs.is_empty() {
        lines.push(format!(
            "- Important directories: {}",
            context
                .important_dirs
                .iter()
                .map(|item| format!("`{item}`"))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if !context.framework_hints.is_empty() {
        lines.push(format!(
            "- Framework hints: {}",
            context.framework_hints.join(", ")
        ));
    }
    if let Some(node) = context.detected.node.as_ref() {
        lines.push(format!(
            "- Node package manager: {}",
            node_package_manager_label(node.package_manager)
        ));
        let mut scripts = Vec::new();
        if node.scripts.has_dev {
            scripts.push("dev");
        }
        if node.scripts.has_build {
            scripts.push("build");
        }
        if node.scripts.has_start {
            scripts.push("start");
        }
        if !scripts.is_empty() {
            lines.push(format!("- Declared Node scripts: {}", scripts.join(", ")));
        }
    }
    if let Some(dev) = context.info.node_dev_entrypoint.as_ref() {
        lines.push(format!("- Suggested dev command: `{}`", dev.join(" ")));
    }
    if let Some(release) = context.info.node_release_entrypoint.as_ref() {
        lines.push(format!(
            "- Suggested release command: `{}`",
            release.join(" ")
        ));
    } else if !context.info.entrypoint.is_empty() {
        lines.push(format!(
            "- Suggested entry command: `{}`",
            context.info.entrypoint.join(" ")
        ));
    }

    lines.push(String::new());
    if context.ambiguities.is_empty() {
        lines.push("## Ambiguities to resolve before generating TOML".to_string());
        lines.push("- No blocking ambiguities were detected from the filesystem scan. Ask a short follow-up question only if you need information that is not justified by the project facts.".to_string());
    } else {
        lines.push("## Ambiguities to resolve before generating TOML".to_string());
        for ambiguity in &context.ambiguities {
            lines.push(format!("- {ambiguity}"));
        }
    }

    lines.push(String::new());
    lines.push("## Schema constraints".to_string());
    for constraint in &context.schema_constraints {
        lines.push(format!("- {constraint}"));
    }

    if !context.decision_rules.is_empty() {
        lines.push(String::new());
        lines.push("## Decision rules".to_string());
        for rule in &context.decision_rules {
            lines.push(format!("- {rule}"));
        }
    }

    lines.push(String::new());
    lines.push("## Task".to_string());
    lines.push("- Review the extracted facts.".to_string());
    lines.push("- Ask every required clarifying question before generating TOML.".to_string());
    lines.push("- Once the user answers, produce the final `capsule.toml`.".to_string());
    lines.push(
        "- Output only a single fenced ```toml code block containing the final TOML.".to_string(),
    );

    lines.join("\n")
}

fn collect_existing(dir: &Path, entries: &[&str]) -> Vec<String> {
    entries
        .iter()
        .filter(|entry| dir.join(entry).exists())
        .map(|entry| (*entry).to_string())
        .collect()
}

fn detect_framework_hints(dir: &Path, detected: &DetectedProject) -> Result<Vec<String>> {
    let mut hints = Vec::new();
    if let Some(node) = detected.node.as_ref() {
        let package_json = read_package_json(dir)?;
        if has_package_dependency(&package_json, "next") {
            hints.push("Next.js".to_string());
        }
        if has_package_dependency(&package_json, "react") {
            hints.push("React".to_string());
        }
        if node.has_hono {
            hints.push("Hono".to_string());
        }
        if node.is_bun {
            hints.push("Bun".to_string());
        }
    }
    Ok(hints)
}

fn detect_ambiguities(
    dir: &Path,
    detected: &DetectedProject,
    info: &ProjectInfo,
    framework_hints: &[String],
) -> Result<Vec<String>> {
    let mut ambiguities = Vec::new();

    if matches!(detected.project_type, ProjectType::Unknown) {
        ambiguities.push(
            "The project type could not be identified confidently. Ask the user what runtime or artifact should be the default target before generating TOML.".to_string(),
        );
    }

    if info.entrypoint.is_empty() {
        ambiguities.push(
            "No reliable entry command or artifact was detected. Ask the user what command or built output should launch the app.".to_string(),
        );
    }

    if framework_hints.iter().any(|hint| hint == "Next.js") && !next_static_export_detected(dir)? {
        ambiguities.push(
            "This looks like a Next.js project, but it is unclear whether the intended deployment is a static export (`out/`) or a dynamic server (`next start` / SSR). Ask the user which mode they want before generating TOML.".to_string(),
        );
    }

    Ok(ambiguities)
}

fn next_static_export_detected(dir: &Path) -> Result<bool> {
    if dir.join("out").exists() {
        return Ok(true);
    }

    for config_name in [
        "next.config.js",
        "next.config.mjs",
        "next.config.cjs",
        "next.config.ts",
    ] {
        let path = dir.join(config_name);
        if !path.exists() {
            continue;
        }
        let content = fs::read_to_string(&path)
            .with_context(|| format!("Failed to read {}", path.display()))?;
        let normalized = content.replace([' ', '\n', '\r', '\t'], "");
        if normalized.contains("output:\"export\"") || normalized.contains("output:'export'") {
            return Ok(true);
        }
    }

    Ok(false)
}

fn read_package_json(dir: &Path) -> Result<serde_json::Value> {
    let path = dir.join("package.json");
    if !path.exists() {
        return Ok(serde_json::Value::Null);
    }
    let content =
        fs::read_to_string(&path).with_context(|| format!("Failed to read {}", path.display()))?;
    Ok(serde_json::from_str(&content).unwrap_or(serde_json::Value::Null))
}

fn has_package_dependency(package_json: &serde_json::Value, dependency: &str) -> bool {
    ["dependencies", "devDependencies", "peerDependencies"]
        .iter()
        .any(|key| {
            package_json
                .get(key)
                .and_then(|deps| deps.as_object())
                .map(|deps| deps.contains_key(dependency))
                .unwrap_or(false)
        })
}

fn node_package_manager_label(package_manager: NodePackageManager) -> &'static str {
    match package_manager {
        NodePackageManager::Bun => "bun",
        NodePackageManager::Npm => "npm",
        NodePackageManager::Pnpm => "pnpm",
        NodePackageManager::Yarn => "yarn",
        NodePackageManager::Unknown => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn detects_next_js_ambiguity_without_static_export_evidence() {
        let tmp = TempDir::new().unwrap();
        fs::write(
            tmp.path().join("package.json"),
            r#"{
  "name": "demo",
  "dependencies": {
    "next": "^15.0.0",
    "react": "^19.0.0"
  },
  "scripts": {
    "build": "next build",
    "start": "next start"
  }
}"#,
        )
        .unwrap();

        let detected = detect::detect_project(tmp.path()).unwrap();
        let info = recipe::project_info_from_detection(&detected).unwrap();
        let context = PromptContext::from_project(tmp.path(), &detected, &info).unwrap();

        assert!(context.framework_hints.iter().any(|hint| hint == "Next.js"));
        assert!(context
            .ambiguities
            .iter()
            .any(|item| item.contains("static export") && item.contains("dynamic server")));
    }

    #[test]
    fn recognizes_next_static_export_from_config() {
        let tmp = TempDir::new().unwrap();
        fs::write(
            tmp.path().join("next.config.js"),
            "export default { output: 'export' };",
        )
        .unwrap();

        assert!(next_static_export_detected(tmp.path()).unwrap());
    }
}
