open System
open System.Net


#r "nuget: Hopac"
#r "nuget: System.Data.SQLite"
// #r "System.Transactions"
// #r @"packages\System.Data.SQLite.Core\lib\net451\System.Data.SQLite.dll"
// #r @"./packages/Hopac/lib/net471/Hopac.dll"

let currentDirectory = __SOURCE_DIRECTORY__

open System.IO
open System.Data.SQLite

type CrawlerData =
    { SITE_DOMAIN : string
      EXCHANGE_DOMAIN : string
      SELLER_ACCOUNT_ID : string
      ACCOUNT_TYPE : string
      TAG_ID : string
      ENTRY_COMMENT : string }

type SQLiteCommand with
    
    static member addWithValue (cmd : SQLiteCommand) (name, value) =
        cmd.Parameters.AddWithValue(name, value) |> ignore
        cmd
    
    static member executeNonQuery (cmd : SQLiteCommand) = cmd.ExecuteNonQuery()

type String with
    static member trim (s : string) = s.Trim()
    static member startsWith (value:string) (s : string) = s.StartsWith value
    static member split (separator : char) (s : string) = s.Split(separator)
    static member join (separator : string) (strings : string []) =
        String.Join(separator, strings)

let createConnectionTo databaseFilename =
    let path = Path.Combine(currentDirectory, databaseFilename)
    match File.Exists path with
    | false -> SQLiteConnection.CreateFile(path)
    | true -> ()
    new SQLiteConnection $"Data Source=%s{path};Version=3;"

let createTableAdsTxt = """
CREATE TABLE IF NOT EXISTS AdsTxt(
       SITE_DOMAIN                  TEXT    NOT NULL,
       EXCHANGE_DOMAIN              TEXT    NOT NULL,
       SELLER_ACCOUNT_ID            TEXT    NOT NULL,
       ACCOUNT_TYPE                 TEXT    NOT NULL,
       TAG_ID                       TEXT    NOT NULL,
       ENTRY_COMMENT                TEXT    NOT NULL,
       UPDATED                      DATE    DEFAULT (datetime('now','localtime')),
    PRIMARY KEY (SITE_DOMAIN,EXCHANGE_DOMAIN,SELLER_ACCOUNT_ID)
)"""

let structureCommand connection =
    use cmd = new SQLiteCommand(createTableAdsTxt, connection)
    cmd.ExecuteNonQuery()

let insertSql =
    """
INSERT INTO AdsTxt (SITE_DOMAIN, EXCHANGE_DOMAIN, SELLER_ACCOUNT_ID, ACCOUNT_TYPE, TAG_ID, ENTRY_COMMENT) 
VALUES (@SITE_DOMAIN, @EXCHANGE_DOMAIN, @SELLER_ACCOUNT_ID, @ACCOUNT_TYPE, @TAG_ID, @ENTRY_COMMENT );"""

let insertCommand connection x =
    try 
        use command = new SQLiteCommand(insertSql, connection)
        [ ("@SITE_DOMAIN", x.SITE_DOMAIN)
          ("@EXCHANGE_DOMAIN", x.EXCHANGE_DOMAIN)
          ("@SELLER_ACCOUNT_ID", x.SELLER_ACCOUNT_ID)
          ("@ACCOUNT_TYPE", x.ACCOUNT_TYPE)
          ("@TAG_ID", x.TAG_ID)
          ("@ENTRY_COMMENT", x.ENTRY_COMMENT) ]
        |> List.fold SQLiteCommand.addWithValue command
        |> SQLiteCommand.executeNonQuery
        |> Ok
    with e -> Error $"Cannot insert: %A{x}. Reason: %s{e.Message}"

let getContent domain =
    async { 
        let wc = {
            new  WebClient() with
            override __.GetWebRequest(uri: Uri) = 
                let w = base.GetWebRequest(uri)
                w.Timeout <- 1000;
                w
        }
        wc.Headers.Add(HttpRequestHeader.Accept, "text/plain")
        try 
            let! fileContent = wc.AsyncDownloadString
                                   (Uri("http://" + domain + "/ads.txt"))
            //printfn "%s processed." domain                          
            return Some(domain, fileContent)
        with _e -> 
            //printfn "%s not processed. Reason: %s" domain _e.Message
            return None
    }

let createCrawlerData siteDomain (line : string) : CrawlerData option =
    let parts = line |> String.split '#'
    Array.tryHead parts
    |> Option.bind (fun data -> 
           let fields =
               data
               |> String.split (',')
               |> Array.map String.trim
               |> Array.filter (String.IsNullOrWhiteSpace >> not)
           try 
               { SITE_DOMAIN = siteDomain
                 EXCHANGE_DOMAIN = fields.[0]
                 SELLER_ACCOUNT_ID = fields.[1]
                 ACCOUNT_TYPE = fields.[2] |> String.map Char.ToUpperInvariant
                 TAG_ID = Array.tryItem 3 fields |> Option.defaultValue ""
                 ENTRY_COMMENT =
                     Array.skip 1 parts
                     |> String.join "\n" }
               |> Some
           with _ -> None)

let doWork domains dbName =
    use connection = createConnectionTo dbName
    connection.Open()
    structureCommand connection |> ignore
    let tran = connection.BeginTransaction()
    let count =
        File.ReadAllLines(Path.Combine(currentDirectory, domains))
        |> Seq.filter (fun d -> Uri.CheckHostName(d) <> UriHostNameType.Unknown)
        |> Seq.take 100
        |> Seq.map getContent
        |> Seq.map (fun x -> 
               async { 
                   let! result = x
                   return result
                          |> Option.map (fun (domain, content) -> 
                                 content
                                 |> String.split '\n'
                                 |> Array.map String.trim
                                 |> Array.filter 
                                        (fun x -> 
                                        not 
                                            (String.IsNullOrWhiteSpace x 
                                             && String.startsWith "#" x))
                                 |> Array.map (createCrawlerData domain)
                                 |> Array.choose id)
               })
        |> Async.Parallel
        |> Async.RunSynchronously
        |> Array.Parallel.choose id
        |> Array.Parallel.collect id
        |> Array.Parallel.map (insertCommand connection)
        |> Array.sumBy (function | Ok c -> 1 | _ -> 0)
    tran.Commit()
    count

#time "on"

doWork ("domains.txt") ("crawler.sqlite3")|> printfn "%i new records inserted."

// Async
// Real: 00:00:24.380, CPU: 00:00:12.328, GC gen0: 37, gen1: 4, gen2: 0

// Hopac
// Real: 00:00:23.631, CPU: 00:00:12.437, GC gen0: 38, gen1: 4, gen2: 0
