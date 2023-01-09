using Xunit;

// Running tests in parallel actually takes more time when we are cleaning the database for majority of tests. It's quicker to just let them run sequentially.
[assembly: CollectionBehavior(CollectionBehavior.CollectionPerAssembly, DisableTestParallelization = true)]