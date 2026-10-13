use std::fs;
use std::path::Path;
use std::process::Command;

#[test]
fn every_scenario_produces_its_expected_output() {
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/e2e/fixtures");
    let mut scenarios: Vec<_> = fs::read_dir(&fixtures)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .filter_map(|name| {
            name.strip_prefix("transactions_")
                .and_then(|suffix| suffix.strip_suffix(".csv"))
                .map(String::from)
        })
        .collect();
    scenarios.sort();
    assert!(!scenarios.is_empty());

    let mut failures = Vec::new();
    for scenario in scenarios {
        let output = Command::new(env!("CARGO_BIN_EXE_processor"))
            .arg(fixtures.join(format!("transactions_{scenario}.csv")))
            .output()
            .unwrap();
        let expected =
            fs::read_to_string(fixtures.join(format!("expected_output_{scenario}.csv"))).unwrap();
        let stdout = String::from_utf8(output.stdout).unwrap();

        if !output.status.success() || stdout != expected {
            failures.push(format!(
                "scenario {scenario}\n--- expected\n{expected}--- got\n{stdout}--- stderr\n{}",
                String::from_utf8_lossy(&output.stderr),
            ));
        }
    }

    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
}
