// SPDX-FileCopyrightText: Copyright © 2020-2023 Serpent OS Developers
//
// SPDX-License-Identifier: MPL-2.0

use std::process;
use std::{io, path::Path};

use boulder::{client, Client};
use clap::Parser;
use thiserror::Error;
use tokio::{
    fs::{create_dir_all, remove_dir_all},
    task,
};

use super::Global;

#[derive(Debug, Parser)]
#[command(about = "Build ... TODO")]
pub struct Command {}

pub async fn handle(_command: Command, global: Global) -> Result<(), Error> {
    let Global {
        moss_root,
        config_dir,
        cache_dir,
    } = global;

    let client = Client::new(config_dir, cache_dir).await?;

    let ephemeral_root = client.cache.join("test-root");
    recreate_dir(&ephemeral_root).await?;

    let mut moss_client = moss::Client::new(&moss_root)
        .await?
        .ephemeral(&ephemeral_root)?;

    moss_client.install(BASE_PACKAGES, true).await?;

    task::spawn_blocking(move || {
        container::run(ephemeral_root, move || {
            let mut child = process::Command::new("/bin/bash")
                .arg("--login")
                .env_clear()
                .env("HOME", "/root")
                .env("PATH", "/usr/bin:/usr/sbin")
                .env("TERM", "xterm-256color")
                .spawn()?;

            child.wait()?;

            Ok(())
        })
    })
    .await
    .expect("join handle")
    .map_err(Error::Container)?;

    Ok(())
}

const BASE_PACKAGES: &[&str] = &[
    "bash",
    "boulder",
    "coreutils",
    "dash",
    "dbus",
    "dbus-broker",
    "file",
    "gawk",
    "git",
    "grep",
    "gzip",
    "inetutils",
    "iproute2",
    "less",
    "linux-kvm",
    "moss",
    "moss-container",
    "nano",
    "neofetch",
    "nss",
    "openssh",
    "procps",
    "python",
    "screen",
    "sed",
    "shadow",
    "sudo",
    "systemd",
    "unzip",
    "util-linux",
    "vim",
    "wget",
    "which",
];

async fn recreate_dir(path: &Path) -> Result<(), Error> {
    if path.exists() {
        remove_dir_all(&path).await?;
    }
    create_dir_all(&path).await?;
    Ok(())
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("container")]
    Container(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("client")]
    Client(#[from] client::Error),
    #[error("moss client")]
    MossClient(#[from] moss::client::Error),
    #[error("moss install")]
    MossInstall(#[from] moss::client::install::Error),
    #[error("io")]
    Io(#[from] io::Error),
}