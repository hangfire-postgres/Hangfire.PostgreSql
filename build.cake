#addin nuget:?package=Newtonsoft.Json&version=9.0.1

//////////////////////////////////////////////////////////////////////
// ARGUMENTS
//////////////////////////////////////////////////////////////////////

var target = Argument("target", "Default");

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
    DotNetCoreRestore();
});

Task("Build")
    .IsDependentOn("Restore")
  .Does(() =>
{
    var settings = new DotNetCoreBuildSettings
    {
        Configuration = "Release"
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
    .IsDependentOn("Test");

RunTarget(target);