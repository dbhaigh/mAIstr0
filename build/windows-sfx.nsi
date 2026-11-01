Unicode true
Name "mAIstr0 ${VERSION}"
OutFile "${OUTPUT_FILE}"
InstallDir "$EXEDIR\mAIstr0_${VERSION}"
RequestExecutionLevel user
SetCompressor /SOLID lzma

Page directory
Page instfiles

Section "Extract"
  SetOutPath "$INSTDIR"
  File /r "${SOURCE_DIR}/*"
SectionEnd
