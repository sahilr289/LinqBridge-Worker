
{ pkgs }: {
  deps = [
    pkgs.nodejs_22

    # X11 + GL stack
    pkgs.xorg.libX11
    pkgs.xorg.libXcomposite
    pkgs.xorg.libXdamage
    pkgs.xorg.libXext
    pkgs.xorg.libXfixes
    pkgs.xorg.libXrandr
    pkgs.xorg.libXrender
    pkgs.xorg.libxcb
    pkgs.libxkbcommon
    pkgs.libdrm
    pkgs.mesa
    pkgs.pciutils  # fixes "libpci missing" in glxtest logs

    # GTK / UI deps
    pkgs.gtk3
    pkgs.glib
    pkgs.gdk-pixbuf
    pkgs.pango
    pkgs.cairo
    pkgs.at-spi2-core
    pkgs.dbus
    pkgs.cups

    # NSS/NSPR (certs, crypto)
    pkgs.nss
    pkgs.nspr

    # Audio (Playwright checks this even headless)
    pkgs.alsa-lib
  ];
}
