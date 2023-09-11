use std::thread::sleep;
use std::time::{Duration, Instant};

use tui::widget::progress;
use tui::{Constraint, Direction, Frame, Handle, Layout};

fn main() {
    tui::run(Program::default(), run).unwrap();
}

fn run(mut handle: Handle<Message>) {
    let now = Instant::now();

    let mut progress = 0;

    loop {
        handle.print(format!("{:?}", now.elapsed()));

        sleep(Duration::from_millis(50));

        progress = (progress + 1) % 100;

        handle.update(Message::Progress(progress));
    }
}

#[derive(Default)]
struct Program {
    progress: f32,
}

enum Message {
    Progress(u8),
}

impl tui::Program for Program {
    const LINES: u16 = 5;

    type Message = Message;

    fn update(&mut self, message: Self::Message) {
        match message {
            Message::Progress(p) => self.progress = p as f32 / 100.0,
        }
    }

    fn draw(&self, frame: &mut Frame) {
        let layout = Layout::new()
            .direction(Direction::Vertical)
            .vertical_margin(1)
            .constraints([
                Constraint::Length(1),
                Constraint::Length(1),
                Constraint::Length(1),
            ])
            .split(frame.size());

        frame.render_widget(
            progress(self.progress, progress::Fill::UpAcross, 20),
            layout[0],
        );
        frame.render_widget(
            progress(self.progress, progress::Fill::AcrossUp, 20),
            layout[2],
        );
    }
}