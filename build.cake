#addin nuget:?package=Newtonsoft.Json&version=9.0.1

//////////////////////////////////////////////////////////////////////
// ARGUMENTS
//////////////////////////////////////////////////////////////////////

var target = Argument("target", "Default");
var npgsqlVersion = Argument("npgsql", "Default");

//////////////////////////////////////////////////////////////////////
// FUNCTIONS
//////////////////////////////////////////////////////////////////////

IEnumerable<string> GetFrameworks(string path) 
{
	return new string[] { "netstandard2.0" };
}

//////////////////////////////////////////////////////////////////////
// TASKS
//////////////////////////////////////////////////////////////////////

Task("Restore")
  .Does(() =>
{
    var settings = new DotNetCoreRestoreSettings
    {
        ArgumentCustomization = args => args.Append($"-p:NpgsqlVersion={npgsqlVersion}")
    };
    DotNetCoreRestore(settings);
});

Task("Build")
    .IsDependentOn("Restore")
  .Does(() =>
{
    var settings = new DotNetCoreBuildSettings
    {
        ArgumentCustomization = args => args.Append($"-p:NpgsqlVersion={npgsqlVersion}"),
        Configuration = "Release",
        NoRestore = true
    };

    var projects = GetFiles("./src/**/*.csproj");
	foreach(var project in projects)
	{
		Information("Found project: {0}", project);
	
		foreach(var framework in GetFrameworks(project.ToString()))
        {
            if (!IsRunningOnWindows() && !framework.StartsWith("netstandard"))
				continue;

            Information("Building: {0} on Framework: {1}", project, framework);
            Information("========");
            settings.Framework = framework;
            DotNetCoreBuild(project.ToString(), settings);
        }
	}
});

Task("Test")
    .IsDependentOn("Build")
  .Does(() =>
{
    var files = GetFiles("tests/**/*.csproj");
    var settings = new DotNetCoreTestSettings
    {
        ArgumentCustomization = args => args.Append($"-p:NpgsqlVersion={npgsqlVersion}")
    };
    foreach(var file in files)
    {
        Information("Testing: {0}", file);
        DotNetCoreTest(file.ToString(), settings);
    }
});

Task("Pack")
//    .IsDependentOn("Test")
    .IsDependentOn("Build")
  .Does(() =>
{
    var settings = new DotNetCorePackSettings
    {
        ArgumentCustomization = args => args.Append($"-p:NpgsqlVersion={npgsqlVersion}"),
        Configuration = "Release",
        OutputDirectory = "publish/",
        NoBuild = true
    };

    var files = GetFiles("src/**/*.csproj");
    foreach(var file in files)
    {
        DotNetCorePack(file.ToString(), settings);
    }
});

Task("Default")
//    .IsDependentOn("Test");
    .IsDependentOn("Pack")

RunTarget(target);