Properties {
    $solution = "Hangfire.PostgreSql.sln"
}
Include "packages\Hangfire.Build.*\tools\psake-common.ps1"

Task Default -Depends Collect

Task Test -Depends Compile -Description "Run unit and integration tests." {
    Run-XunitTests "Hangfire.PostgreSql.Tests"
}

Task Merge -Depends Test -Description "Run ILMerge /internalize to merge assemblies." {
    # Remove `*.pdb` file to be able to prepare NuGet symbol packages.
    Remove-Item ((Get-SrcOutputDir "Hangfire.PostgreSql") + "\Dapper.pdb")

    Merge-Assembly "Hangfire.PostgreSql" @("Dapper")
}

Task Collect -Depends Merge -Description "Copy all artifacts to the build folder." {
    Collect-Assembly "Hangfire.PostgreSql" "Net45"

    Collect-Content "content\readme.txt"
    Collect-Tool "src\Hangfire.PostgreSql\Install.v3.sql"
}

Task Pack -Depends Collect -Description "Create NuGet packages and archive files." {
    $version = Get-BuildVersion

    Create-Archive "Hangfire-PostgreSql-$version"
    Create-Package "Hangfire.PostgreSql" $version
}

