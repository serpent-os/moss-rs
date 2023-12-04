// SPDX-FileCopyrightText: Copyright © 2020-2023 Serpent OS Developers
//
// SPDX-License-Identifier: MPL-2.0

use std::collections::HashSet;

use dag::Dag;
use futures::{StreamExt, TryFutureExt};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{package, Provider, Registry};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Id(u64);

enum ProviderFilter {
    /// Must be installed
    InstalledOnly(Provider),

    /// Filter the lookup to current selection scope
    Selections(Provider),

    // Look beyond installed/selections
    All(Provider),
}

enum Lookup {
    InstalledOnly,
    Global,
}

/// A Transaction is used to modify one system state to another
#[derive(Clone, Debug)]
pub struct Transaction<'a> {
    // Unique identifier - baked only for commited transactions
    id: Option<Id>,

    // Bound to a registry
    registry: &'a Registry,

    // unique set of package ids
    packages: Dag<package::Id>,
}

/// Construct a new Transaction wrapped around the underlying Registry
/// At this point the registry is initialised and we can probe the installed
/// set.
pub(super) fn new(registry: &Registry) -> Result<Transaction<'_>, Error> {
    Ok(Transaction {
        id: None,
        registry,
        packages: Dag::default(),
    })
}

/// Populate the transaction on initialisation
pub(super) async fn new_with_installed(
    registry: &Registry,
    incoming: Vec<package::Id>,
) -> Result<Transaction<'_>, Error> {
    let mut tx = new(registry)?;
    tx.update(incoming, Lookup::InstalledOnly).await?;
    Ok(tx)
}

impl<'a> Transaction<'a> {
    /// Add a package to this transaction
    pub async fn add(&mut self, incoming: Vec<package::Id>) -> Result<(), Error> {
        self.update(incoming, Lookup::Global).await
    }

    /// Remove a set of packages and their reverse dependencies
    pub async fn remove(&mut self, packages: Vec<package::Id>) -> Result<(), Error> {
        // Get transposed subgraph
        let transposed = self.packages.transpose();
        let subgraph = transposed.subgraph(&packages);

        // For each node, remove it from transaction graph
        subgraph.iter_nodes().for_each(|package| {
            // Remove that package
            self.packages.remove_node(package);
        });

        Ok(())
    }

    /// Return the package IDs in the fully baked configuration
    pub fn finalize(&self) -> impl Iterator<Item = &package::Id> + '_ {
        self.packages.topo()
    }

    /// Update internal package graph with all incoming packages & their deps
    async fn update(&mut self, incoming: Vec<package::Id>, lookup: Lookup) -> Result<(), Error> {
        let mut items = incoming;

        loop {
            if items.is_empty() {
                break;
            }
            let mut next = vec![];
            for check_id in items.iter() {
                // Ensure node is added and get it's index
                let check_node = self.packages.add_node_or_get_index(check_id.clone());

                // Grab this package in question
                let matches = self.registry.by_id(check_id).collect::<Vec<_>>().await;
                let package = matches
                    .first()
                    .ok_or(Error::NoCandidate(check_id.clone().into()))?;
                for dependency in package.meta.dependencies.iter() {
                    let provider = Provider {
                        kind: dependency.kind.clone(),
                        name: dependency.name.clone(),
                    };

                    // Now get it resolved
                    let search = match lookup {
                        Lookup::Global => self.resolve_installation_provider(provider).await?,
                        Lookup::InstalledOnly => {
                            self.resolve_provider(ProviderFilter::InstalledOnly(provider))
                                .await?
                        }
                    };

                    // Add dependency node
                    let need_search = !self.packages.node_exists(&search);
                    let dep_node = self.packages.add_node_or_get_index(search.clone());

                    // No dag node for it previously
                    if need_search {
                        next.push(search.clone());
                    }

                    // Connect w/ edges (rejects cyclical & duplicate edges)
                    self.packages.add_edge(check_node, dep_node);
                }
            }
            items = next;
        }

        match self.list_conflicts(lookup).await {
            Ok(conflicts) if conflicts.is_empty() => {}
            Ok(conflicts) => todo!(),
            Err(err) => return Err(err),
        }

        Ok(())
    }

    /// Check if the current installation causes conflicts.
    ///
    /// This can be reduced to a 2-SAT
    /// problem. Each package has two possible states: installed or uninstalled, representing
    /// true or false respectively; we also have constraints between packages, namely dependency
    /// relations and conflict relations.
    ///
    /// The process to find if a valid solution to this 2-SAT problem exists looks like this:
    /// 1. Each node is split into two nodes, node1 and node2. node1 represents true, the
    ///    condition where the package is installed, and node2
    ///    indicates false, the condition where the package is uninstalled.
    ///    Therefore, a graph of N nodes (packages) will become a graph of 2N
    ///    nodes.
    /// 2. Say package A depends on package B and conflicts with package C. Then, an edge is
    ///    constructed from nodeA1 to nodeB1, indicating that to install A, B must be installed as
    ///    well. An edge is also constructed from nodeA1 to nodeC2, indicating that to install A,
    ///    C must be uninstalled.
    /// 3. Now, condense this graph into its strongly connected components. Each connected
    ///    components means that the condition represented by every node in this component must
    ///    be simultaneously satisfied. Hence, if for a package X, nodeX1 and nodeX2 are in the
    ///    same connected component, this means package X must be both installed **and**
    ///    uninstalled to satisfy dependency and conflict relations, which is clearly impossible
    ///    and we report this conflict.
    async fn list_conflicts(
        &self,
        lookup: Lookup,
    ) -> Result<Vec<(package::Id, Vec<package::Id>)>, Error> {
        let mut graph: Dag<(package::Id, bool)> = Dag::default();

        for pkg_id in self.packages.iter_nodes() {
            let pkg_t = graph.add_node_or_get_index((pkg_id.clone(), true));

            for dependency in self.packages.neighbors_outgoing(pkg_id) {
                let dependency_t = graph.add_node_or_get_index((dependency.clone(), true));
                graph.add_edge(pkg_t, dependency_t);
            }

            // Grab this package in question
            let matches = self.registry.by_id(pkg_id).collect::<Vec<_>>().await;
            let package = matches
                .first()
                .ok_or(Error::NoCandidate(pkg_id.clone().into()))?;

            for conflict in package.meta.conflicts.iter() {
                let provider = Provider {
                    kind: conflict.kind.clone(),
                    name: conflict.name.clone(),
                };

                // Now get the conflicting provider resolved
                let result = match lookup {
                    Lookup::Global => self.resolve_installation_provider(provider).await,
                    Lookup::InstalledOnly => {
                        self.resolve_provider(ProviderFilter::InstalledOnly(provider))
                            .await
                    }
                };

                match result {
                    Ok(conflict_id) => {
                        let conflict_f = graph.add_node_or_get_index((conflict_id, false));
                        graph.add_edge(pkg_t, conflict_f);
                    }
                    Err(Error::NoCandidate(_)) => {}
                    // Is this how I rethrow/rereturn an error in Rust?
                    Err(e) => return Err(e),
                }
            }
        }

        let components = graph.scc();
        let mut conflicts: Vec<(package::Id, Vec<package::Id>)> = vec![];
        for component in components {
            let mut visited: HashSet<package::Id> = HashSet::default();

            for condition_index in component {
                let (package_id, installed) = graph.get_node_from_index(condition_index);

                if visited.contains(package_id) {
                    let reasons: Vec<package::Id> = graph
                        .neighbors_outgoing(&(package_id.clone(), false))
                        .filter_map(|revdep| {
                            if let (revdep_id, true) = revdep {
                                match visited.contains(revdep_id) {
                                    true => Some(revdep_id.clone()),
                                    false => None,
                                }
                            } else {
                                None
                            }
                        })
                        .collect();
                    conflicts.push((package_id.clone(), reasons))
                }
                visited.insert(package_id.clone());
            }
        }

        Ok(conflicts)
    }

    /// Attempt to resolve the filterered provider
    async fn resolve_provider(&self, filter: ProviderFilter) -> Result<package::Id, Error> {
        match filter {
            ProviderFilter::All(provider) => self
                .registry
                .by_provider(&provider, package::Flags::AVAILABLE)
                .boxed()
                .next()
                .await
                .map(|p| p.id.clone())
                .ok_or(Error::NoCandidate(provider.to_string())),
            ProviderFilter::InstalledOnly(provider) => self
                .registry
                .by_provider(&provider, package::Flags::INSTALLED)
                .boxed()
                .next()
                .await
                .map(|p| p.id.clone())
                .ok_or(Error::NoCandidate(provider.to_string())),
            ProviderFilter::Selections(provider) => self
                .registry
                .by_provider(&provider, package::Flags::NONE)
                .filter_map(|f| async {
                    if self.packages.node_exists(&f.id) {
                        Some(f)
                    } else {
                        None
                    }
                })
                .boxed()
                .next()
                .await
                .map(|p| p.id.clone())
                .ok_or(Error::NoCandidate(provider.to_string())),
        }
    }

    // Try all strategies to resolve a provider for installation
    async fn resolve_installation_provider(
        &self,
        provider: Provider,
    ) -> Result<package::Id, Error> {
        self.resolve_provider(ProviderFilter::Selections(provider.clone()))
            .or_else(|_| async {
                self.resolve_provider(ProviderFilter::InstalledOnly(provider.clone()))
                    .await
            })
            .or_else(|_| async {
                self.resolve_provider(ProviderFilter::All(provider.clone()))
                    .await
            })
            .await
    }
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("No such name: {0}")]
    NoCandidate(String),

    #[error("Not yet implemented")]
    NotImplemented,

    #[error("meta db")]
    Database(#[from] crate::db::meta::Error),
}
