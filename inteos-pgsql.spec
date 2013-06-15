# Bacula RPM spec file
#
# Copyright (C) 2011-2012 Inteos Sp. z o.o.

# Platform Build Configuration
# TODO: merge all plugins into one spec file

# basic defines for every build
%define _release           3
%define _version           2.1
%define _beeversion	   6.2.1
%define _packager Radoslaw Korzeniewski <radekk@inteos.pl>
%define manpage_ext gz

# Don't strip binaries
%define __os_install_post %{nil}
%define __debug_install_post %{nil}
%define debug_package %{nil} 

%define base_package_name bacula-enterprise

Summary: Bacula - The Network Backup Solution
Name: bacula-inteos-pgsql-plugin
Version: %{_version}_%{_beeversion}
Release: %{_release}
Group: System Environment/Daemons
License: AGPLv3
BuildRoot: %{_tmppath}/%{name}-root
URL: http://www.inteos.pl/
Vendor: Inteos Sp. z o.o.
Packager: %{_packager}
Prefix: %{_prefix}
Distribution: Bacula Inteos Postgresql Plugin

Source0: pgsql-%{_version}_%{_beeversion}.tar.bz2

Requires: %{base_package_name}-client = %{_beeversion}

# define the basic package description
%define blurb Bacula Inteos Plugin - The Leading Open Source Backup Solution.
%define blurb2 Bacula Inteos Plugin allows you to backup and restore 
%define blurb3 a PostgreSQL Database with hot/online mode

Summary: Bacula PostgreSQL Plugin - The Network Backup Solution
Group: System Environment/Daemons

%description
%{blurb}

%{blurb2}
%{blurb3}

This is Bacula Inteos PostgreSQL plugin.

%prep
cd $RPM_BUILD_DIR
rm -rf %{name}-%{_version}_%{_beeversion}
mkdir -p %{name}-%{_version}_%{_beeversion}
cd %{name}-%{_version}_%{_beeversion}
tar xjvf $RPM_SOURCE_DIR/pgsql-%{_version}_%{_beeversion}.tar.bz2

%install
cd $RPM_BUILD_ROOT
mv $RPM_BUILD_DIR/%{name}-%{_version}_%{_beeversion}/opt $RPM_BUILD_ROOT
export QA_RPATHS=$[ 0x0001|0x0010|0x0002 ]


%files
%config(noreplace) /opt/bacula/etc/pgsql.conf.example
%defattr(755,root,-)
/opt/bacula/bin/pgsql-archlog
/opt/bacula/bin/pgsql-restore
/opt/bacula/plugins/pgsql-fd.so

%changelog
* Fri Apr 24 2013 Radoslaw Korzeniewski <radekk@inteos.pl>
- Update to version 1.9
- Build for PgSQL 9.x
- add RPM Package
