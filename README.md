# LRC Database Builder

このリポジトリは、数百万の楽曲データを誇る「LRCHub」の分散された歌詞リポジトリ群をGitHub Actions上で並列クロールし、高速な単一のSQLiteデータベース（`lyrics.db.br`）としてコンパイル・自動生成するための Rust 製バッチツールです。

## 🚀 使い方（初期セットアップ）

GitHubのインフラをフル活用して全自動の歌詞データベース生成基盤を作るための手順です。

### Step 1: GitHubでの準備

1. ご自身のGitHubアカウントで、**空の新しいリポジトリ** を作成してください（名前例: `lrc-db-builder`）。PublicでもPrivateでも構いません。
2. リポジトリの **Settings（設定）** タブを開きます。
3. 左サイドバーから **Actions** ＞ **General** をクリックします。
4. 画面を下へスクロールし、**Workflow permissions** の項目を `Read and write permissions` に変更して `Save`（保存）してください。
   - ※ これを行わないと、Actionsが完成したDBをRelease（アセット）として自動アップロードできません。

### Step 2: このコードをGitHubへアップロード

お手元のPC（コマンドプロンプトまたはPowerShell）で、この `lrc-db-builder` フォルダを開き、以下のコマンドを順に実行してプッシュします。

```bash
# フォルダ内でGitを初期化
git init

# 全てのファイル（ソースコードとActionsの設定）を追加
git add .

# コミットを作成
git commit -m "Initial commit for LRC DB Builder"

# 作成したGitHubリポジトリのURLを登録（ご自身のURLに書き換えてください）
git remote add origin https://github.com/ryyr-ry/lrc-db-builder.git

# GitHubへコードを送信
git branch -M main
git push -u origin main
```

### Step 3: 自動構築の実行（GitHub Actions）

コードをPushすると、設定ファイル（`.github/workflows/build.yml`）が自動で読み込まれます。

1. GitHubリポジトリの **Actions** タブを開きます。
2. 左側の `Build LRC Database` というワークフローをクリックします。
3. 右側の `Run workflow` ボタンを押し、緑色の `Run workflow` をクリックして手動で処理を開始させます。
4. 数分待つと処理が完了し、リポジトリの **Releases** ページに `lyrics.db.br` という圧縮済みデータベースファイルが自動的に生成・配置されます。

以降は**毎日深夜**に自動で最新のデータがクロールされ、Releasesのデータベースが最新のものへと勝手に更新（ローリングパブリッシュ）され続けます。
