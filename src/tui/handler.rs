use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use tui_input::backend::crossterm::EventHandler;

use super::app::App;

pub fn handle_key_event(app: &mut App, event: KeyEvent) {
    if app.input_mode {
        match event.code {
            KeyCode::Esc => {
                app.input_mode = false;
            }
            KeyCode::Enter => {
                app.input_mode = false;
            }
            _ => {
                app.query.handle_event(&crossterm::event::Event::Key(event));
                app.mark_query_changed();
            }
        }
        return;
    }

    match (event.code, event.modifiers) {
        (KeyCode::Char('q'), _) | (KeyCode::Esc, _) => {
            app.should_quit = true;
        }
        (KeyCode::Char('/'), _) => {
            app.input_mode = true;
        }
        (KeyCode::Char('j'), _) | (KeyCode::Down, _) => {
            app.move_down();
        }
        (KeyCode::Char('k'), _) | (KeyCode::Up, _) => {
            app.move_up();
        }
        (KeyCode::Char('c'), KeyModifiers::CONTROL) => {
            app.should_quit = true;
        }
        (KeyCode::Enter, _) => {
            app.accepted_scoped_id = app.selected_scoped_id();
            app.should_quit = true;
        }
        (KeyCode::Char('i'), _) => {
            app.hint = app
                .selected_scoped_id()
                .map(|s| format!("Install: ato install {}", s));
        }
        (KeyCode::Char('m'), _) => {
            app.show_manifest = !app.show_manifest;
            if !app.show_manifest {
                app.hint = Some("Manifest view: off".to_string());
            } else {
                app.hint = Some("Manifest view: on".to_string());
            }
        }
        _ => {}
    }
}
