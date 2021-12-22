using Newtonsoft.Json.Linq;
using RestSharp;
using RestSharp.Authenticators;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace _3d_new_mvc
{
    class Orient
    {
        public static JObject GetFullGalaxyOrient(string[] args)
        {
            string url = args[0],
                query = args[1];

            JObject startNodes = docall(url,query);
            return startNodes;
        }

        public static JObject GetLineagePaths(
            JObject jsonFromOrient, string url)
        {
            try
            {
                JArray reults = new JArray();
                int i = 0;
                string queryInsert = "";

                ParallelOptions po = new ParallelOptions();
                //po.MaxDegreeOfParallelism = 4;
                //Parallel.ForEach(jsonFromOrient["result"],po, (jtStartNode) =>
                foreach (JToken jtStartNode in jsonFromOrient["result"])
                {
                   
                  string nodesOnPath = "";
                    string startNode = jtStartNode["@rid"].ToString();
                    //get path for node
                    string query = "traverse out() from " + startNode;
                    JObject startNodePath = docall(url, query);
                    int resCnt = ((Newtonsoft.Json.Linq.JContainer)startNodePath["result"]).Count;
                    if (resCnt > 0)
                    {
                        //Parallel.ForEach(startNodePath["result"], po, (jtPath) =>
                        foreach (JToken jtPath in startNodePath["result"])
                        {
                            string newRid = jtPath["@rid"].ToString();
                            if (startNode != newRid)
                            {
                                nodesOnPath = nodesOnPath + newRid.Replace("#","").Replace(":","") + ", ";
                            }
                        }
                        nodesOnPath = nodesOnPath.Remove(nodesOnPath.Length - 2);
                        nodesOnPath = "[" + nodesOnPath + "]";
                        //
                        queryInsert = queryInsert + 
                              "('" + startNode.Replace("#", "").Replace(":", "") + "', " + nodesOnPath + "), ";
                        Console.WriteLine(i);
                    }
                    i++;
                }
                //INSERT INTO StartNodesPaths
                queryInsert = queryInsert.Remove(queryInsert.Length - 2);
                queryInsert = "INSERT INTO StartNodesPaths(BaseNode, NodesOnPath) VALUES  " + queryInsert;
                JObject insertRes = docall(url, queryInsert);
                Console.WriteLine(i);
                Console.WriteLine(insertRes["result"]);
                reults.Add(JArray.Parse(insertRes["result"].ToString()));

                JObject jsonForOrientPyFormat =JObject.Parse("{results:" + reults + "}");
                //todo.. return errors

                 return jsonForOrientPyFormat;
            }   
            catch (Exception ex)
            {
                return JObject.Parse("{\"errors\":\"" + ex.ToString() + "\"}");
            }
        }

        public static JObject WriteJsonToOrient(string url, string query)
        {
            try
            {
                JObject wrtieResponse = docall(url, query);
                return wrtieResponse;
            }
            catch (Exception ex)
            {
                return JObject.Parse("{\"errors\":\""+ ex.ToString() +"\"}");
            }
            
        }

        public static JObject docall(string url, string command)
        {
            var client = new RestClient(url);
            client.Timeout = -1;
            var request = new RestRequest(Method.POST);
            request.AddHeader("Content-Type", "application/json");
            request.AddHeader("Authorization", "Basic YWRtaW46YWRtaW4=");
            client.Authenticator = new HttpBasicAuthenticator("root", "root");
            request.AddParameter("application/json", "{\r\n  \"command\": \"" + command + "\"\r\n}", ParameterType.RequestBody);
            IRestResponse response = client.Execute(request);
            JObject o = JObject.Parse(response.Content);
            return o;
            //Console.WriteLine(response.Content);
            //System.IO.File.WriteAllText(@"C:\Users\itaik\Documents\orienttests\V.json", response.Content);
        }
    }
}
