// SPDX-FileCopyrightText: Copyright © 2020-2023 Serpent OS Developers
//
// SPDX-License-Identifier: MPL-2.0

use petgraph::{
    algo::tarjan_scc,
    prelude::DiGraph,
    visit::{Dfs, Topo, Walker},
    Direction,
};

use self::subgraph::subgraph;

mod subgraph;

/// NodeIndex as employed in moss-rs usage
pub type NodeIndex = petgraph::prelude::NodeIndex<u32>;

/// Simplistic encapsulation of petgraph APIs to provide
/// suitable mechanisms to empower transaction code
#[derive(Debug, Clone)]
pub struct Dag<N>(DiGraph<N, (), u32>);

impl<N> Default for Dag<N> {
    fn default() -> Self {
        Self(DiGraph::default())
    }
}

impl<N> Dag<N>
where
    N: Clone + PartialEq,
{
    /// Construct a new Dag
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_node_from_index(&self, index: NodeIndex) -> &N {
        &self.0[index]
    }

    /// Adds node N to the graph and returns the index.
    /// If N already exists, it'll return the index of that node.
    pub fn add_node_or_get_index(&mut self, node: N) -> NodeIndex {
        if let Some(index) = self.get_index(&node) {
            index
        } else {
            self.0.add_node(node)
        }
    }

    /// Returns true if the node exists
    pub fn node_exists(&self, node: &N) -> bool {
        self.get_index(node).is_some()
    }

    /// Remove node
    pub fn remove_node(&mut self, node: &N) -> Option<N> {
        if let Some(index) = self.get_index(node) {
            self.0.remove_node(index)
        } else {
            None
        }
    }

    /// Add an edge from a to b
    pub fn add_edge(&mut self, a: NodeIndex, b: NodeIndex) -> bool {
        let a_node = &self.0[a];

        // prevent cycle (b connects to a)
        if self.dfs(b).any(|n| n == a_node) {
            return false;
        }

        // don't add edge if it alread exists
        if self.0.find_edge(a, b).is_some() {
            return false;
        }

        // We're good, add it
        self.0.add_edge(a, b, ());

        true
    }

    pub fn iter_nodes(&self) -> impl Iterator<Item = &'_ N> {
        self.0.node_indices().map(|i| &self.0[i])
    }

    pub fn neighbors_incoming(&self, node: &N) -> impl Iterator<Item = &'_ N> {
        self.0
            .neighbors_directed(self.get_index(node).unwrap(), Direction::Incoming)
            .map(|neighbor| &self.0[neighbor])
    }

    pub fn neighbors_outgoing(&self, node: &N) -> impl Iterator<Item = &'_ N> {
        self.0
            .neighbors_directed(self.get_index(node).unwrap(), Direction::Outgoing)
            .map(|neighbor| &self.0[neighbor])
    }

    /// Perform a depth-first search, given the start index
    pub fn dfs(&self, start: NodeIndex) -> impl Iterator<Item = &'_ N> {
        let dfs = Dfs::new(&self.0, start);

        dfs.iter(&self.0).map(|i| &self.0[i])
    }

    /// Perform a toplogical sort
    pub fn topo(&self) -> impl Iterator<Item = &'_ N> {
        let topo = Topo::new(&self.0);

        topo.iter(&self.0).map(|i| &self.0[i])
    }

    /// Transpose the graph, returning the clone
    pub fn transpose(&self) -> Self {
        let mut transposed = self.0.clone();
        transposed.reverse();
        Self(transposed)
    }

    pub fn scc(&self) -> Vec<Vec<NodeIndex>> {
        // Note: tarjan and kosaraju have the same time complexity, but tarjan
        // has a better constant factor. They should produce the equivalent
        // result regardless.
        tarjan_scc(&self.0)
    }

    pub fn scc_nodes(&self) -> Vec<Vec<&N>> {
        tarjan_scc(&self.0)
            .iter()
            .map(|component| component.iter().map(|node_id| &self.0[*node_id]).collect())
            .collect()
    }

    /// Split the graph at the given start node(s) - returning a new graph
    pub fn subgraph(&self, starting_nodes: &[N]) -> Self {
        Self(subgraph(&self.0, starting_nodes))
    }

    /// Return the index for node of type N
    pub fn get_index(&self, node: &N) -> Option<NodeIndex> {
        self.0.node_indices().find(|i| self.0[*i] == *node)
    }
}
