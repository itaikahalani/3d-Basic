using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using _3d_new_mvc.Models;
using Newtonsoft.Json.Linq;

namespace _3d_new_mvc.Controllers
{
    public class HomeController : Controller
    {
        private readonly ILogger<HomeController> _logger;

        public HomeController(ILogger<HomeController> logger)
        {
            _logger = logger;
        }

        public IActionResult Index()
        {
            return View();
        }

        public IActionResult Privacy()
        {
            return View();
        }

        [ResponseCache(Duration = 0, Location = ResponseCacheLocation.None, NoStore = true)]
        public IActionResult Error()
        {
            return View(new ErrorViewModel { RequestId = Activity.Current?.Id ?? HttpContext.TraceIdentifier });
        }


        ///////////////////////////////////////////////////////////////////////////////////////////////////////
        
        public string GetFullGalaxy()
        {
            string OrientHost = "localhost";
            string OrientPort = "2480";

            //ReadOrientConfig();
            string url = "http://" + OrientHost + ":" + OrientPort + "/command/C2C2/sql";

            string[] args = new[] { url, " TRAVERSE * FROM (select * from v where ContainerObjectName = 'Sales by Geo') " };
            //string[] args = new[] { url, "  TRAVERSE out() FROM (select * from column where in().size() = 0 ) " };

            JObject GetFullGalaxyNodes = Orient.GetFullGalaxyOrient(args);

            JArray NodesJson = JArray.Parse("[]");
            JArray LinksJson = JArray.Parse("[]");

            JArray NodesLinksJson = new JArray();

            foreach (var itm1 in GetFullGalaxyNodes["result"])
            {
                //Vertex
                if ((string)itm1["@class"] == "COLUMN")
                {
                    string @rid = (string)itm1["@rid"];
                    string ObjectGuid = (string)itm1["@rid"];
                    string ObjectGuid1 = (string)itm1["guid"];
                    string ContainerToolType = (string)itm1["ToolName"];
                    string ObjectId = (string)itm1["id"];
                    string ContainerObjectName = (string)itm1["ContainerObjectName"];
                    string ContainerObjectPath = (string)itm1["ContarinerObjectPath"];
                    string ObjectName = (string)itm1["ObjectName"];
                    string ObjectType = (string)itm1["ObjectType"];
                    string ServerName = (string)itm1[""];
                    string Db = (string)itm1["DBName"];
                    string Schema = (string)itm1["SchemaName"];
                    string Table = (string)itm1["TableName"];
                    string Layer = (string)itm1["LayerName"];
                    string ToolName = (string)itm1["ToolName"];
                    string ToolType = (string)itm1["ToolType"];
                    string ControlFlowPath = (string)itm1["ControlFlowName"];
                    JArray in_DataFlow = (JArray)itm1["in_DataFlow"];
                    JArray out_DataFlow = (JArray)itm1["out_DataFlow"];


                    if (Table == "" || ToolName == "COGNOS" || ToolName == "SSIS")
                    {
                        Table = Layer;
                        Schema = ControlFlowPath;
                        Db = ContainerObjectName;
                    };

                    if (Db != null)
                    {
                        Db = Db.ToUpper();
                        Schema = Schema.ToUpper();
                        Table = Table.ToUpper();
                    }
                    if (ControlFlowPath != null)
                    { ControlFlowPath = ControlFlowPath.ToUpper(); }
                       

                    if (ObjectId != null)
                    {
                        ObjectId = ObjectId.Replace(@"\", @"\\");
                    }
                    if (ContainerObjectPath != null)
                    {
                        ContainerObjectPath = ContainerObjectPath.Replace(@"\", @"\\");
                    }

                    //groups
                    string group = "";
                    if(ToolType == "ETL")
                    {
                        group = "1";
                    }
                    if (ToolType == "DB")
                    {
                        group = "2";
                    }
                    if (ToolType == "REPORT")
                    {
                        group = "3";
                    }


                    JObject Vline = JObject.Parse(
                        " { " +
                        //"\"TYPE\": \"node\", " +
                        //"\"END\": \"IS_KEY\", " +
                        //"\"@rid\": \"" + @rid + "\"," +
                        //"\"ToolName\": \"" + ToolName + "\"," +
                        //"\"GUID\": \"" + ObjectGuid + "\"," +
                        "\"id\": \"" + @rid + "\"," +
                        "\"group\": \"" + group + "\"," +
                        //"\"ContainerToolType\": \"" + ContainerToolType + "\"," +
                        //"\"ObjectId\": \"" + ObjectId + "\"," +
                        //"\"ContainerObjectName\": \"" + ContainerObjectName + "\"," +
                        //"\"ContainerObjectPath\": \"" + ContainerObjectPath + "\"," +
                        //"\"ControlFlowPath\": \"" + ControlFlowPath + "\"," +
                        //"\"name\": \"" + ObjectName + "\"," +
                        //"\"ObjectType\": \"" + ObjectType + "\"," +
                        //"\"Server\": \"" + ServerName + "\"," +
                        //"\"DB\": \"" + Db + "\"," +
                        //"\"Schema\": \"" + Schema + "\"," +
                        //"\"Layer\": \"" + Layer + "\"," +
                        //"\"Table\": \"" + Table + "\" "+
                        " }"
                       );
                    NodesJson.Add(Vline);

                    //Edge
                    //for  in()/out()
                    NodesLinksJson.Add(in_DataFlow);
                    NodesLinksJson.Add(out_DataFlow);
                }

                //Edge
                //for travers
                if ((string)itm1["@class"] == "DATAFLOW")
                {
                    string @rid = (string)itm1["@rid"];
                    string from = (string)itm1["out"];
                    string to = (string)itm1["in"];
                    //string fromText = "in";
                    //string toText = "out";

                    JObject Eline = JObject.Parse(
                           "{ "+
                         //  " \"TYPE\": \"link\"," +
                        //"\"@rid\": \"" + @rid + "\"," +
                        "\"source\": \"" + from + "\"," +
                        "\"target\": \"" + to + "\"," +
                        //"\"fromText\": \"" + fromText + "\"," +
                        //"\"toText\": \"" + toText + "\"," +
                        "\"value\": \"3\"}");
                    LinksJson.Add(Eline);
                }
                //NodesJson.Add(jo);
            }

            JObject finalJson =
                new JObject(
                    new JProperty("nodes", new JArray(NodesJson)),
                    new JProperty("links", new JArray(LinksJson))
                    );
            return finalJson.ToString();
        }

        public string GetFullGalaxyTables()
        {
            string OrientHost = "10.6.0.13";
            string OrientPort = "2480";
            string dbName = "DEMOQA_PROD";

            //ReadOrientConfig();
            string url = "http://" + OrientHost + ":" + OrientPort + "/command/"+ dbName + "/sql";

            string[] args = new[] { url, " TRAVERSE * FROM #20:159" };
          
            JObject GetFullGalaxyNodes = Orient.GetFullGalaxyOrient(args);

            JArray NodesJson = JArray.Parse("[]");
            JArray LinksJson = JArray.Parse("[]");

            foreach (var itm1 in GetFullGalaxyNodes["result"])
            {
                //Vertex
                if ((string)itm1["@class"] == "LINEAGEOBJECT")
                {
                    string rid = (string)itm1["@rid"];
                    string ObjectGuid = (string)itm1["@rid"];
                    string ObjectGuid1 = (string)itm1["guid"];
                    string ContainerToolType = (string)itm1["ToolName"];
                    string ObjectId = (string)itm1["id"];
                    string ContainerObjectName = (string)itm1["ContainerObjectName"];
                    string ContainerObjectPath = (string)itm1["ContarinerObjectPath"];
                    string ObjectName = (string)itm1["ObjectName"];
                    string ObjectType = (string)itm1["ObjectType"];
                    string ServerName = (string)itm1[""];
                    string Db = (string)itm1["DBName"];
                    string Schema = (string)itm1["SchemaName"];
                    string Table = (string)itm1["TableName"];
                    string Layer = (string)itm1["LayerName"];
                    string ToolName = (string)itm1["ToolName"];
                    string ToolType = (string)itm1["ToolType"];
                    string ControlFlowPath = (string)itm1["ControlFlowName"];
                    JArray in_DataFlow = (JArray)itm1["in_LINEAGEFLOW"];
                    JArray out_DataFlow = (JArray)itm1["out_LINEAGEFLOW"];


                    if (Table == "" || ToolName == "COGNOS" || ToolName == "SSIS")
                    {
                        Table = Layer;
                        Schema = ControlFlowPath;
                        Db = ContainerObjectName;
                    };

                    if (Db != null)
                    {
                        Db = Db.ToUpper();
                        Schema = Schema.ToUpper();
                        Table = Table.ToUpper();
                    }
                    if (ControlFlowPath != null)
                    { ControlFlowPath = ControlFlowPath.ToUpper(); }


                    if (ObjectId != null)
                    {
                        ObjectId = ObjectId.Replace(@"\", @"\\");
                    }
                    if (ContainerObjectPath != null)
                    {
                        ContainerObjectPath = ContainerObjectPath.Replace(@"\", @"\\");
                    }
                    if (ObjectName != null)
                    {
                        ObjectName = ObjectName.Replace(@"\", @"\\");
                    }

                    //groups
                    string group = "";
                    if (ToolType == "ETL")
                    {
                        group = "1";
                    }
                    if (ToolType == "DATABASE")
                    {
                        group = "2";
                    }
                    if (ToolType == "REPORT")
                    {
                        group = "3";
                    }

                    int inoutCnt = 1;
                    if (in_DataFlow != null && out_DataFlow != null)
                    {
                        inoutCnt = in_DataFlow.Count + out_DataFlow.Count;
                    }
                    if (in_DataFlow != null && out_DataFlow == null)
                    {
                        inoutCnt = in_DataFlow.Count;
                    }
                    if (in_DataFlow == null && out_DataFlow != null)
                    {
                        inoutCnt = out_DataFlow.Count;
                    }

                    JObject Vline = JObject.Parse(
                        " { " +
                        //"\"TYPE\": \"node\", " +
                        //"\"END\": \"IS_KEY\", " +
                        "\"rid\": \"" + @rid + "\"," +
                        "\"ToolName\": \"" + ToolName + "\"," +
                        //"\"GUID\": \"" + ObjectGuid + "\"," +
                        "\"id\": \"" + @rid + "\"," +
                        "\"group\": \"" + group + "\"," +
                        "\"ContainerToolType\": \"" + ContainerToolType + "\"," +
                        //"\"ObjectId\": \"" + ObjectId + "\"," +
                        "\"ContainerObjectName\": \"" + Table + "\"," +
                        "\"ContainerObjectPath\": \"" + Schema + "\"," +
                        "\"ControlFlowPath\": \"" + ControlFlowPath + "\"," +
                        "\"Name\": \"" + ObjectName + "\"," +
                        "\"ToolType\": \"" + ToolType + "\"," +
                        "\"ObjectType\": \"" + ObjectType + "\"," +
                        //"\"Server\": \"" + ServerName + "\"," +
                        //"\"DB\": \"" + Db + "\"," +
                        //"\"Schema\": \"" + Schema + "\"," +
                        //"\"Layer\": \"" + Layer + "\"," +
                        //"\"Table\": \"" + Table + "\" "+
                        "\"inoutCnt\": \"" + inoutCnt + "\" " +
                        " }"
                       );
                    NodesJson.Add(Vline);
                }
            }

            foreach (var itm1 in GetFullGalaxyNodes["result"])
            {
                if ((string)itm1["@class"] == "LINEAGEFLOW")
                {
                    string @rid = (string)itm1["@rid"];
                    string from = (string)itm1["out"];
                    string to = (string)itm1["in"];
                    //string fromText = "in";
                    //string toText = "out";

                    JObject Eline = JObject.Parse(
                           "{ " +
                        //  " \"TYPE\": \"link\"," +
                        //"\"@rid\": \"" + @rid + "\"," +
                        "\"source\": \"" + from + "\"," +
                        "\"target\": \"" + to + "\"," +
                        //"\"fromText\": \"" + fromText + "\"," +
                        //"\"toText\": \"" + toText + "\"," +
                        "\"value\": \"3\"}");
                    LinksJson.Add(Eline);
                }
            }

            JObject finalJson =
                new JObject(
                    new JProperty("nodes", new JArray(NodesJson)),
                    new JProperty("links", new JArray(LinksJson))
                    );
            return finalJson.ToString();
        }

        public string GetFullGalaxyTablesInOutAll()
        {
            string OrientHost = "10.6.0.13";
            string OrientPort = "2480";
            string dbName = "DEMOQA_PROD";

            //ReadOrientConfig();
            string url = "http://" + OrientHost + ":" + OrientPort + "/command/" + dbName + "/sql";

            string[] args = new[] { url, " TRAVERSE out() FROM (select * from V where ObjectContainer = 'Load DWH') " };

            JObject GetFullGalaxyNodes = Orient.GetFullGalaxyOrient(args);

            JArray NodesJson = JArray.Parse("[]");
            JArray LinksJson = JArray.Parse("[]");

            JArray in_DataFlow = new JArray();
            JArray out_DataFlow = new JArray() ;

            JArray in_DataFlow_j = new JArray();
            JArray out_DataFlow_j = new JArray();

            foreach (var itm1 in GetFullGalaxyNodes["result"])
            {
                //Vertex
                if ((string)itm1["@class"] == "LINEAGEOBJECT")
                {
                    string rid = (string)itm1["@rid"];
                    string ObjectGuid = (string)itm1["@rid"];
                    string ObjectGuid1 = (string)itm1["guid"];
                    string ContainerToolType = (string)itm1["ToolName"];
                    string ObjectId = (string)itm1["id"];
                    string ContainerObjectName = (string)itm1["ContainerObjectName"];
                    string ContainerObjectPath = (string)itm1["ContarinerObjectPath"];
                    string ObjectName = (string)itm1["ObjectName"];
                    string ObjectType = (string)itm1["ObjectType"];
                    string ServerName = (string)itm1[""];
                    string Db = (string)itm1["DBName"];
                    string Schema = (string)itm1["SchemaName"];
                    string Table = (string)itm1["TableName"];
                    string Layer = (string)itm1["LayerName"];
                    string ToolName = (string)itm1["ToolName"];
                    string ToolType = (string)itm1["ToolType"];
                    string ControlFlowPath = (string)itm1["ControlFlowName"];
                    in_DataFlow = (JArray)itm1["in_LINEAGEFLOW"];
                    out_DataFlow = (JArray)itm1["out_LINEAGEFLOW"];

                    in_DataFlow_j.Add(in_DataFlow);
                    in_DataFlow_j.Add(in_DataFlow);

                    if (Table == "" || ToolName == "COGNOS" || ToolName == "SSIS")
                    {
                        Table = Layer;
                        Schema = ControlFlowPath;
                        Db = ContainerObjectName;
                    };

                    if (Db != null)
                    {
                        Db = Db.ToUpper();
                        Schema = Schema.ToUpper();
                        Table = Table.ToUpper();
                    }
                    if (ControlFlowPath != null)
                    { ControlFlowPath = ControlFlowPath.ToUpper(); }


                    if (ObjectId != null)
                    {
                        ObjectId = ObjectId.Replace(@"\", @"\\");
                    }
                    if (ContainerObjectPath != null)
                    {
                        ContainerObjectPath = ContainerObjectPath.Replace(@"\", @"\\");
                    }
                    if (ObjectName != null)
                    {
                        ObjectName = ObjectName.Replace(@"\", @"\\");
                    }

                    //groups
                    string group = "";
                    if (ToolType == "ETL")
                    {
                        group = "1";
                    }
                    if (ToolType == "DATABASE")
                    {
                        group = "2";
                    }
                    if (ToolType == "REPORT")
                    {
                        group = "3";
                    }

                    int inoutCnt = 1;
                    if (in_DataFlow != null && out_DataFlow != null)
                    {
                        inoutCnt = in_DataFlow.Count + out_DataFlow.Count;
                    }
                    if (in_DataFlow != null && out_DataFlow == null)
                    {
                        inoutCnt = in_DataFlow.Count;
                    }
                    if (in_DataFlow == null && out_DataFlow != null)
                    {
                        inoutCnt = out_DataFlow.Count;
                    }

                    JObject Vline = JObject.Parse(
                        " { " +
                        //"\"TYPE\": \"node\", " +
                        //"\"END\": \"IS_KEY\", " +
                        "\"rid\": \"" + @rid + "\"," +
                        "\"ToolName\": \"" + ToolName + "\"," +
                        //"\"GUID\": \"" + ObjectGuid + "\"," +
                        "\"id\": \"" + @rid + "\"," +
                        "\"group\": \"" + group + "\"," +
                        "\"ContainerToolType\": \"" + ContainerToolType + "\"," +
                        //"\"ObjectId\": \"" + ObjectId + "\"," +
                        "\"ContainerObjectName\": \"" + Table + "\"," +
                        "\"ContainerObjectPath\": \"" + Schema + "\"," +
                        "\"ControlFlowPath\": \"" + ControlFlowPath + "\"," +
                        "\"Name\": \"" + ObjectName + "\"," +
                        "\"ToolType\": \"" + ToolType + "\"," +
                        "\"ObjectType\": \"" + ObjectType + "\"," +
                        //"\"Server\": \"" + ServerName + "\"," +
                        //"\"DB\": \"" + Db + "\"," +
                        //"\"Schema\": \"" + Schema + "\"," +
                        //"\"Layer\": \"" + Layer + "\"," +
                        //"\"Table\": \"" + Table + "\" "+
                        "\"inoutCnt\": \"" + inoutCnt + "\" " +
                        " }"
                       );
                    NodesJson.Add(Vline);
                }
            }

            if (in_DataFlow_j != null)
            {
                foreach (var IN in in_DataFlow_j)
                {
                    args = new[] { url, " select * from e where @rid in " + IN.ToString() + " " };
                    JObject GetFullGalaxyLinksIn = Orient.GetFullGalaxyOrient(args);
                    foreach (var itm1 in GetFullGalaxyLinksIn["result"])
                    {
                        string from = (string)itm1["in"];
                        string to = (string)itm1["out"];
                        //string fromText = "in";
                        //string toText = "out";

                        JObject Eline = JObject.Parse(
                               "{ " +
                            //  " \"TYPE\": \"link\"," +
                            //"\"@rid\": \"" + @rid + "\"," +
                            "\"source\": \"" + from + "\"," +
                            "\"target\": \"" + to + "\"," +
                            //"\"fromText\": \"" + fromText + "\"," +
                            //"\"toText\": \"" + toText + "\"," +
                            "\"value\": \"3\"}");
                        LinksJson.Add(Eline);

                    }
                }
            }

            if (out_DataFlow != null)
            {
                foreach (var OUT in out_DataFlow)
                {
                    args = new[] { url, " select * from e where @rid in " + OUT.ToString() + " " };
                    JObject GetFullGalaxyLinksOut = Orient.GetFullGalaxyOrient(args);
                    foreach (var itm1 in GetFullGalaxyLinksOut["result"])
                    {
                        string from = (string)itm1["in"];
                        string to = (string)itm1["out"];
                        //string fromText = "in";
                        //string toText = "out";

                        JObject Eline = JObject.Parse(
                               "{ " +
                            //  " \"TYPE\": \"link\"," +
                            //"\"@rid\": \"" + @rid + "\"," +
                            "\"source\": \"" + from + "\"," +
                            "\"target\": \"" + to + "\"," +
                            //"\"fromText\": \"" + fromText + "\"," +
                            //"\"toText\": \"" + toText + "\"," +
                            "\"value\": \"3\"}");
                        LinksJson.Add(Eline);

                    }
                }
            }


            JObject finalJson =
                new JObject(
                    new JProperty("nodes", new JArray(NodesJson)),
                    new JProperty("links", new JArray(LinksJson))
                    );
            return finalJson.ToString();
        }

    }
}
