#addin "Cake.Json"

//////////////////////////////////////////////////////////////////////
// ARGUMENTS
//////////////////////////////////////////////////////////////////////

var target = Argument("target", "Default");

//////////////////////////////////////////////////////////////////////
// FUNCTIONS
//////////////////////////////////////////////////////////////////////

IEnumerable<string> GetFrameworks(string path) 
{
    var projectJObject = DeserializeJsonFromFile<JObject>(path);
    foreach(var prop in ((JObject)projectJObject["frameworks"]).Properties()) 
    {
        yield return prop.Name;   
    }    
}

//////////////////////////////////////////////////////////////////////
// TASKS
//////////////////////////////////////////////////////////////////////

Task("Restore")
  .Does(() =>
{
    DotNetCoreRestore("src/\" \"tests/");
});

Task("Build")
    .IsDependentOn("Restore")
  .Does(() =>
{
    var settings = new DotNetCoreBuildSettings
    {
        Configuration = "Release"
    };

    var projects = GetFiles("./src/**/project.json");
	foreach(var project in projects)
	{
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
        OutputDirectory = "publish/",
        NoBuild = true
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