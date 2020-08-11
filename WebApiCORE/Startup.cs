using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.IO;
using WebApICore.Base;

namespace WebApiCORE
{
    public class Startup
    {
        public Startup(IConfiguration configuration)
        {

            try 
            {
                //读取json配置文件
                string text = File.ReadAllText("Config/webapi_config.json");
                dynamic obj = JsonConvert.DeserializeObject(text);
                //初始化线程池
                string golden_username = obj.GoldenUsername;
                string golden_password = obj.GoldenPassword;
                string golden_host = obj.GoldenHost;
                string golden_port = obj.GoldenPort;
                int golden_pool_size = obj.PoolSize;
                var res = GoldenAPI.ConnectionPool.Init(golden_username, golden_password, golden_host, golden_port, golden_pool_size);
                if (res)
                {
                    Console.WriteLine("连接池成功");
                }
                else 
                {
                    Console.WriteLine("连接池失败");
                }
                //标签点缓存池开启
                TagPointManager.Init();
                //快照缓存池开启
                SnapShotManager.Init();
            }
            catch (Exception e) 
            {
                Console.WriteLine("初始化失败");
            }
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {
            services.AddControllers();
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }

            app.UseRouting();

            app.UseAuthorization();

            app.UseEndpoints(endpoints =>
            {
                endpoints.MapControllers();
            });
        }
    }
}
