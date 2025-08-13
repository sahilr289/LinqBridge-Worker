{ pkgs }: {
  deps = [
    pkgs.nodejs-16_x
    pkgs.nodePackages.typescript-language-server
    pkgs.yarn
    pkgs.replitPackages.jest
    pkgs.cairo
    pkgs.pango
    pkgs.libxcb
    pkgs.xorg.libX11
    pkgs.xorg.libXext
    pkgs.xorg.libXrandr
    pkgs.xorg.libXcomposite
    pkgs.xorg.libXcursor
    pkgs.xorg.libXdamage
    pkgs.xorg.libXfixes
    pkgs.xorg.libXi
    pkgs.xorg.libXrender
    pkgs.gtk3
    pkgs.gdk-pixbuf
    pkgs.glib
    pkgs.alsa-lib
    pkgs.dbus
    pkgs.atk
  ];
}