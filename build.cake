var target = Argument("target", "Default");

Task("Restore")
  .Does(() =>
{
    DotNetCoreRestore("src/\" \"tests/");
});

Task("Build")
    .IsDependentOn("Restore")
  .Does(() =>
{
    DotNetCoreBuild("src/**/project.json\" \"tests/**/project.json");
});

Task("Test")
    .IsDependentOn("Build")
  .Does(() =>
{
    var files = GetFiles("tests/**/project.json");
    foreach(var file in files)
    {
        DotNetCoreTest(file.ToString());
    }
});

Task("Pack")
    .IsDependentOn("Test")
  .Does(() =>
{
    var settings = new DotNetCorePackSettings
    {
        Configuration = "Release",
        OutputDirectory = "publish/"
    };

    var files = GetFiles("src/**/project.json");
    foreach(var file in files)
    {
        DotNetCorePack(file.ToString(), settings);
    }
});

Task("Default")
    .IsDependentOn("Test");

RunTarget(target);