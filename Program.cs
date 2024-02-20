using Npgsql;
using OpenQA.Selenium;
using OpenQA.Selenium.Chrome;
using System.Globalization;
using System.Text;
using TakePricesCSharp;

List<DadosExportacao> dados = [];

// URL's para busca
string url_boi = "https://www.melhorcambio.com/boi-hoje";
string url_soja = "https://www.melhorcambio.com/soja-hoje";
string url_milho = "https://www.melhorcambio.com/milho-hoje";
string url_cafe = "https://www.melhorcambio.com/cafe-hoje";
string url_suino = "https://www.melhorcambio.com/suino-hoje";
string url_bezerro = "https://www.melhorcambio.com/bezerro-hoje";

ChromeOptions options = new ChromeOptions();
options.AddArgument("--headless");
options.AddArgument("--disable-gpu");

IWebDriver driver = new ChromeDriver(options);

#region Extração dos dados

Console.WriteLine("Iniciando extração de dados! Aguarde...");

// Boi
var preco_boi = ExtrairPreco(url_boi, driver, "Boi");
dados.Add(preco_boi);


// Soja
var preco_soja = ExtrairPreco(url_soja, driver, "Soja");
dados.Add(preco_soja);

// Milho
var preco_milho = ExtrairPreco(url_milho, driver, "Milho");
dados.Add(preco_milho);

// Café
var preco_cafe = ExtrairPreco(url_cafe, driver, "Café");
dados.Add(preco_cafe);

// Suino
var preco_suino = ExtrairPreco(url_suino, driver, "Suino");
dados.Add(preco_suino);

// Bezerro
var preco_bezerro = ExtrairPreco(url_bezerro, driver, "Bezerro");
dados.Add(preco_bezerro);

ExportarParaCsv(dados, @"extracao/precos_ativos_bruto.csv");

#endregion

#region Transformação dos dados

Console.WriteLine("Iniciando transformação de dados! Aguarde...");

var dadosTransformados = CarregarECalcular(@"extracao/precos_ativos_bruto.csv");
ExportarParaCsv(dadosTransformados, @"transformacao/precos_ativos.csv");

#endregion

#region Carregamento dos dados

Console.WriteLine("Iniciando carregamento dos dados! Aguarde...");

SalvarDados(dadosTransformados);

Console.WriteLine("Carregamento dos dados finalizados!");

#endregion

Thread.Sleep(10000);
driver.Quit();

static DadosExportacao ExtrairPreco(string url, IWebDriver driver, string ativo)
{
    driver.Navigate().GoToUrl(url);

    Thread.Sleep(5000);
    var preco_extraido = driver.FindElement(By.XPath("//*[@id=\"comercial\"]")).GetAttribute("value");

    DadosExportacao preco = new();
    preco.Ativo = ativo;
    preco.Preco = preco_extraido;
    preco.DataHora = DateTime.UtcNow;

    return preco;
}

static void ExportarParaCsv(List<DadosExportacao> dados, string caminhoArquivo)
{
    string diretorio = Path.GetDirectoryName(caminhoArquivo);
    if (!Directory.Exists(diretorio))
    {
        Directory.CreateDirectory(diretorio);
    }

    // Iniciar a escrita do arquivo
    using (StreamWriter file = new StreamWriter(caminhoArquivo, false, Encoding.UTF8))
    {
        // Escrever o cabeçalho do CSV
        file.WriteLine("Ativo;Preco;DataHora");

        // Escrever os dados
        foreach (var dado in dados)
        {
            file.WriteLine($"{dado.Ativo};{dado.Preco};{dado.DataHora:yyyy-MM-dd HH:mm:ss}");
        }
    }

    Console.WriteLine($"Dados exportados com sucesso para {caminhoArquivo}");
}

static List<DadosExportacao> CarregarECalcular(string caminhoArquivo)
{
    List<DadosExportacao> dadosTransformados = new List<DadosExportacao>();

    using (StreamReader sr = new StreamReader(caminhoArquivo))
    {
        string linha;
        bool primeiraLinha = true;

        while ((linha = sr.ReadLine()) != null)
        {
            if (primeiraLinha)
            {
                primeiraLinha = false; // Pular o cabeçalho
                continue;
            }

            var colunas = linha.Split(';');

            var preco_formatado = colunas[1].Replace(".", "");

            DadosExportacao dado = new()
            {
                Ativo = colunas[0],
                Preco = preco_formatado,
                DataHora = DateTime.ParseExact(colunas[2], "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture)
            };

            dadosTransformados.Add(dado);
        }
    }

    return dadosTransformados;
}

static void SalvarDados(List<DadosExportacao> dadosExportacao)
{
    string connectionString = "Host=silly.db.elephantsql.com;Username=obqsthil;Password=rt8Q20TgMc8QSx2tlTDiy4wbOcgDgh6y;Database=obqsthil";

    using (var conn = new NpgsqlConnection(connectionString))
    {
        conn.Open();

        using (var cmd = new NpgsqlCommand())
        {
            cmd.Connection = conn;

            foreach (var dado in dadosExportacao)
            {
                Guid guid = Guid.NewGuid();
                cmd.CommandText = "INSERT INTO precos_ativos (id, ativo, preco, data_hora) VALUES (@id, @ativo, @preco, @datahora)";
                cmd.Parameters.AddWithValue("@id", guid);
                cmd.Parameters.AddWithValue("@ativo", dado.Ativo);
                cmd.Parameters.AddWithValue("@preco", decimal.Parse(dado.Preco));
                cmd.Parameters.AddWithValue("@datahora", dado.DataHora);
                cmd.ExecuteNonQuery();
                cmd.Parameters.Clear();
            }
        }
    }
}