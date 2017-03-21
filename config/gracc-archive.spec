Name:           gracc-archive
Version:        1.0
Release:        1%{?dist}
Summary:        GRACC Archive

License:        ASL 2.0
URL:            https://opensciencegrid.github.io/gracc/
Source0:        gracc-archive-%{version}.tar.gz
BuildArch:      noarch

BuildRequires:  python-setuptools
BuildRequires:  systemd
BuildRequires:  python-srpm-macros 
BuildRequires:  python-rpm-macros 
BuildRequires:  python2-rpm-macros 
BuildRequires:  epel-rpm-macros
BuildRequires:  systemd
Requires:       python2-pika
Requires:       python-toml
Requires(pre):  shadow-utils

%description
GRACC Archive Agent


%pre
getent group gracc >/dev/null || groupadd -r gracc
getent passwd gracc >/dev/null || \
    useradd -r -g gracc -d /tmp -s /sbin/nologin \
    -c "GRACC Services Account" gracc
exit 0

%prep
%setup -q


%build
%{py2_build}


%install
%{py2_install}


install -d -m 0755 $RPM_BUILD_ROOT/%{_sysconfdir}/graccarchive/config.d/
install -m 0744 config/gracc-archive.toml $RPM_BUILD_ROOT/%{_sysconfdir}/graccarchive/config.d/gracc-archive.toml
install -d -m 0755 $RPM_BUILD_ROOT/%{_unitdir}
install -m 0744 config/graccarchive.service $RPM_BUILD_ROOT/%{_unitdir}/



%files
%defattr(-, gracc, gracc)
%{python2_sitelib}/graccarchive
%{python2_sitelib}/graccarchive-%{version}-py2.?.egg-info
%attr(755, root, root) %{_bindir}/*
%{_unitdir}/graccarchive.service
%config %{_sysconfdir}/graccarchive/config.d/gracc-archive.toml

%doc



%changelog
* Tue Dec 13 2016 Derek Weitzel <dweitzel@cse.unl.edu> 1.0-1
- Initial build

