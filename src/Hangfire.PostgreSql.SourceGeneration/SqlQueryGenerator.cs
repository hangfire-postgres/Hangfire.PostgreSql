using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;
using System.Text;

namespace Hangfire.PostgreSql.SourceGeneration;

[Generator]
public class SqlQueryGenerator : IIncrementalGenerator
{
  private static readonly DiagnosticDescriptor _interpolatedStringError = new(
    id: "HFPG001",
    title: "Interpolated strings are not allowed in GetQuery",
    messageFormat: "Interpolated strings are not allowed in GetQuery. Use a literal string, formattable string with arguments or the full callback instead.",
    category: "Usage",
    DiagnosticSeverity.Error,
    isEnabledByDefault: true);

  public void Initialize(IncrementalGeneratorInitializationContext context)
  {
    IncrementalValuesProvider<InvocationExpressionSyntax?> queryStrings = context.SyntaxProvider
      .CreateSyntaxProvider(
        predicate: static (node, _) => node is InvocationExpressionSyntax { Expression: MemberAccessExpressionSyntax { Name.Identifier.Text: "GetQuery" } },
        transform: static (context, _) => context.Node as InvocationExpressionSyntax)
      .Where(static invocation => invocation != null);

    context.RegisterSourceOutput(queryStrings.Collect(), static (context, invocations) => {
      StringBuilder queryMappings = new();
      foreach (InvocationExpressionSyntax? invocation in invocations.Where(x => x is { ArgumentList.Arguments.Count: > 0 }))
      {
        ExpressionSyntax argumentExpression = invocation!.ArgumentList.Arguments[0].Expression;

        if (argumentExpression is InterpolatedStringExpressionSyntax)
        {
          Location location = argumentExpression.GetLocation();
          context.ReportDiagnostic(Diagnostic.Create(_interpolatedStringError, location));
          continue;
        }

        if (argumentExpression is LiteralExpressionSyntax literal && literal.IsKind(SyntaxKind.StringLiteralExpression))
        {
          string key = GenerateKey(literal.Token.ValueText);
          string value = ProcessStringExpression(literal.Token.ValueText);
          queryMappings.AppendLine(invocation.ArgumentList.Arguments.Count == 1
            ? $"""
                     AddQuery("{key}", schemaName => $@"{value}");
               """
            : $"""
                     AddQuery("{key}", (schemaName, args) => string.Format($@"{value}", args));
               """);
        }
      }

      string containerCode =
        $$"""
          namespace Hangfire.PostgreSql.GeneratedQueries
          {
            public static class QueryContainer
            {
              private static readonly System.Collections.Generic.Dictionary<string, System.Func<string, string>> SingleParamQueries =
                new System.Collections.Generic.Dictionary<string, System.Func<string, string>>();
          
              private static readonly System.Collections.Generic.Dictionary<string, System.Func<string, object[], string>> MultiParamQueries =
                new System.Collections.Generic.Dictionary<string, System.Func<string, object[], string>>();
          
              static QueryContainer()
              {
          {{queryMappings}}
              }
          
              public static System.Func<string, string, string> GetQuery() => (schemaName, query) =>
              {
                string key = GenerateKey(query);
                if (SingleParamQueries.TryGetValue(key, out var func))
                {
                  return func(schemaName);
                }
                throw new System.InvalidOperationException($"Query not found: {query}");
              };
          
              public static System.Func<string, string, object[], string> GetQueryWithArgs() => (schemaName, query, args) =>
              {
                string key = GenerateKey(query);
                if (MultiParamQueries.TryGetValue(key, out var func))
                {
                  return func(schemaName, args);
                }
                throw new System.InvalidOperationException($"Query not found: {query}");
              };
          
              private static void AddQuery(string key, System.Func<string, string> queryFunc)
              {
                if (SingleParamQueries.TryGetValue(key, out var existingFunc))
                {
                  string existingQuery = existingFunc.Invoke("schema");
                  string newQuery = queryFunc.Invoke("schema");
          
                  if (!string.Equals(existingQuery, newQuery, System.StringComparison.Ordinal))
                  {
                    throw new System.InvalidOperationException(
                      $"Duplicate key detected for different queries. Key: {key}, Existing Query: {existingQuery}, New Query: {newQuery}");
                  }
                }
                else
                {
                  SingleParamQueries[key] = queryFunc;
                }
              }
          
              private static void AddQuery(string key, System.Func<string, object[], string> queryFunc)
              {
                if (MultiParamQueries.TryGetValue(key, out var existingFunc))
                {
                  string existingQuery = existingFunc.Invoke("schema", new object[10]);
                  string newQuery = queryFunc.Invoke("schema", new object[10]);
          
                  if (!string.Equals(existingQuery, newQuery, System.StringComparison.Ordinal))
                  {
                    throw new System.InvalidOperationException(
                      $"Duplicate key detected for different queries. Key: {key}, Existing Query: {existingQuery}, New Query: {newQuery}");
                  }
                }
                else
                {
                  MultiParamQueries[key] = queryFunc;
                }
              }
          
              private static string GenerateKey(string query)
              {
                using System.Security.Cryptography.SHA256 sha256 = System.Security.Cryptography.SHA256.Create();
                byte[] hash = sha256.ComputeHash(System.Text.Encoding.UTF8.GetBytes(query));
                return "Query_" + System.BitConverter.ToString(hash).Replace("-", "");
              }
            }
          }
          """;

      context.AddSource("QueryContainer.g.cs", SourceText.From(containerCode, Encoding.UTF8));
    });
  }

  private static string GenerateKey(string query)
  {
    // If this needs update, make sure to also update the generated code.
    using System.Security.Cryptography.SHA256? sha256 = System.Security.Cryptography.SHA256.Create();
    byte[] hash = sha256.ComputeHash(System.Text.Encoding.UTF8.GetBytes(query));
    return "Query_" + System.BitConverter.ToString(hash).Replace("-", "");
  }

  private static string ProcessStringExpression(string input)
  {
    string processed = input
      .Replace("\"", "\"\"")
      .Replace("hangfire.", "{schemaName}.")
      .Replace("\r\n", " ")
      .Replace("\n", " ")
      .Replace("\t", " ");
    // Remove extra spaces
    processed = System.Text.RegularExpressions.Regex.Replace(processed, @"\s+", " ");
    // Replace format indexers
    return System.Text.RegularExpressions.Regex.Replace(processed, @"\{(\d+)\}", "{{$1}}");
  }
}

