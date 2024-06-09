with import <nixpkgs> {  
config.allowUnfree = true; 
}; let
  # sklearn-deap = python3Packages.sklearn-deap.overrideAttrs (_: { doCheck = false; });
  masterPkg = (import (fetchTarball "https://github.com/NixOS/nixpkgs/archive/master.tar.gz") {
    nixpkgs.config = {
      allowUnfree = true;
      allowUnfreePredicate = pkg: builtins.elem (lib.getName pkg) [
        "google-chrome"                                        
      ];                                                       
    };
  });
  old = (import (fetchTarball "https://github.com/NixOS/nixpkgs/archive/23.11.tar.gz") {
    nixpkgs.config = {
      allowUnfree = true;
      allowUnfreePredicate = pkg: builtins.elem (lib.getName pkg) [
        "google-chrome"                                        
      ];                                                       
    };
  });
in
pkgs.mkShell
rec {
  name = "python-deps";

  buildInputs = with old.python3Packages; [
    python 
    pip

    venvShellHook
    # jupyter-all
    # jupyter
    pandas
    numpy
    matplotlib
    sklearn-deap
    # masterPkg.python3Packages.sklearn-deap
    # notebook

    # PyExecJS

    numpy
    requests
    flask
    flask-babel
    browser-cookie3
    aiohttp
    pycrypto
    taglib
    openssl
    git
    libxml2
    libxslt
    libzip
    zlib
    pkgs.stdenv.cc.cc.lib
  ];

  venvDir = "venv";

  LD_LIBRARY_PATH = "${pkgs.stdenv.cc.cc.lib}/lib/";


  postShellHook = ''
    export PYTHONPATH=`pwd`/venv/lib/python3.11/site-packages:$PYTHONPATH
    pip install -r requirements.txt
    # jupyter contrib nbextension install --user
  '';
}
